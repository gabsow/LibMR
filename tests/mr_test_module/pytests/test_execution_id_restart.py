import threading
import time

from RLTest import Defaults

from common import MRTestDecorator, TimeLimit, initialiseCluster


@MRTestDecorator(skipTest=Defaults.num_shards == 1)
def testExecutionIdsDoNotCollideAfterShardRestart(env, conn):
    initiator = env.getConnection(shardId=1)

    conn.execute_command('set', 'restart-id-key', 'value')

    def start_old_execution():
        try:
            initiator.execute_command('lmrtest.unevenwork')
        except Exception:
            pass

    old_thread = threading.Thread(target=start_old_execution)
    old_thread.start()
    time.sleep(0.5)

    env.envRunner.shards[0].stopEnv()
    old_thread.join(timeout=5)
    env.assertFalse(old_thread.is_alive())
    env.envRunner.shards[0].startEnv()
    initialiseCluster(env)

    restarted = env.getConnection(shardId=1)
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            if all('cluster_state:ok' in c.execute_command('cluster', 'info')
                   for c in (env.getConnection(shardId=1), env.getConnection(shardId=2))):
                break
        except Exception:
            pass
        time.sleep(0.1)
    with TimeLimit(10):
        result = restarted.execute_command('lmrtest.readallkeys')
    env.assertContains('restart-id-key', result)
