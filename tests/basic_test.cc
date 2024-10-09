#include "simple_thread_pool.h"

#include "test_common.h"

#include <set>

#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

using namespace simple_thread_pool;

size_t tid_simple() {
#ifdef __linux__
    return (uint32_t)syscall(SYS_gettid);
#else
    return std::hash<std::thread::id>{}(std::this_thread::get_id()) & 0xff;
#endif
}

int basic_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    // Initialize thread pool.
    ThreadPoolMgr mgr;
    mgr.init(opt);

    std::mutex mm;

    // Run 20 asynchronous tasks.
    for (size_t ii=0; ii<20; ++ii) {
        mgr.addTask( [ii, &mm](const TaskResult& ret) {
            std::lock_guard<std::mutex> l(mm);
            TestSuite::_msgt("[%02zx] hello world %d\n",
                             tid_simple(), ii);
        } );
    }

    // Wait until all tasks are done.
    TestSuite::sleep_sec(1);

    // Shutdown thread pool.
    mgr.shutdown();
    return 0;
}

int recurring_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    // Initialize thread pool.
    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    size_t count = 0;

    // Register a recurring timer whose interval is 100 ms.
    std::shared_ptr<TaskHandle> tt =
        mgr.addTask( [&count](const TaskResult& ret) {
                         TestSuite::_msgt("[%02zx] hello world %zu\n",
                                          tid_simple(), count++);
                     },
                     100000,
                     TaskType::RECURRING );
    // Wait 1 second, timer should be fired 10 times.
    TestSuite::sleep_sec(1);

    // Cancel the timer.
    tt->cancel();
    TestSuite::_msgt("canceled timer\n");

    // Wait another 1 second, now timer should be fired.
    TestSuite::sleep_sec(1);

    // Shutdown thread pool.
    mgr.shutdown();
    return 0;
}

void relay_func(size_t* count, ThreadPoolMgr* mgr, const TaskResult& ret) {
    TestSuite::_msgt("[%02zx] hello world %zu\n",
                     tid_simple(), (*count)++);
    if (*count < 10) {
        // Register an one-time timer whose interval is 100 ms.
        // Once the timer fires, it re-register itself up to 10 times.
        mgr->addTask( std::bind(relay_func,
                                count,
                                mgr,
                                std::placeholders::_1),
                      100000 );
    }
}

int relay_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    // Initialize thread pool.
    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");
    size_t count = 0;

    // Fire timer task.
    relay_func(&count, &mgr, TaskResult());

    // Wait 2 seconds.
    TestSuite::sleep_sec(2);

    // Shutdown thread pool.
    mgr.shutdown();
    return 0;
}

struct FastRelayCtx {
    std::set<size_t> storage;
    bool asyncScheduleNext = true;
    EventAwaiter ea;
};

void fast_relay_func(FastRelayCtx* ctx, ThreadPoolMgr* mgr, const TaskResult& ret) {
    size_t num_entries = ctx->storage.size();
    if (num_entries < 1000000) {
        std::hash<size_t> h_func;
        size_t h_val = h_func(num_entries);
        ctx->storage.insert(h_val);

        if (num_entries % 100000 == 0) {
            TestSuite::_msgt("[%02zx] inserted %zu\n", tid_simple(), num_entries);
        }

        if (ctx->asyncScheduleNext) {
            // Register the next immediate task.
            mgr->addTask( std::bind(fast_relay_func,
                                    ctx,
                                    mgr,
                                    std::placeholders::_1) );
        }
    } else {
        ctx->ea.invoke();
    }
}

/**
 * @param mode
 *        0: sync mode.
 *        1: async with active worker = false.
 *        2: async with active worker = true.
 */
int fast_relay_test(int mode) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = 2;
    if (mode == 1) {
        opt.activeWorkerMode = false;
    } else if (mode == 2) {
        opt.activeWorkerMode = true;
    }

    // Initialize thread pool.
    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");
    TestSuite::Timer tt;

    FastRelayCtx relay_ctx;
    if (mode == 0) {
        relay_ctx.asyncScheduleNext = false;
        for (size_t ii = 0; ii < 1000000; ++ii) {
            fast_relay_func(&relay_ctx, &mgr, TaskResult());
        }
    } else {
        // Fire timer task.
        fast_relay_func(&relay_ctx, &mgr, TaskResult());
        // Wait 3 seconds.
        relay_ctx.ea.wait_ms(3000);
    }

    uint64_t elapsed_us = tt.getTimeUs();
    TestSuite::_msgt("%.1f executions/s",
                     relay_ctx.storage.size() * 1000000.0 / elapsed_us);

    // Shutdown thread pool.
    mgr.shutdown();
    return 0;
}

int reschedule_one_time_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    // Initialize thread pool.
    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    // Register a timer whose interval is 5 seconds.
    std::shared_ptr<TaskHandle> tt =
        mgr.addTask( [](const TaskResult& ret) {
                         TestSuite::_msgt("[%02zx] hello world\n",
                                          tid_simple());
                     },
                     5000000 );
    // Wait 1 second, timer shouldn't be fired in the meantime.
    TestSuite::sleep_sec(1);

    // Re-schedule the timer, now interval is 100 ms.
    tt->reschedule(100000);
    TestSuite::_msgt("rescheduled 5 sec -> 100 ms\n");

    // Wait another 1 second, now timer should be fired.
    TestSuite::sleep_sec(1);

    // Shutdown thread pool.
    mgr.shutdown();
    return 0;
}

int reschedule_recurring_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    // Initialize thread pool.
    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    // Register a recurring timer whose interval is 200 ms.
    size_t count = 0;
    std::shared_ptr<TaskHandle> tt =
        mgr.addTask( [&count](const TaskResult& ret) {
                         TestSuite::_msgt("[%02zx] hello world %zu\n",
                                          tid_simple(), count++);
                     },
                     200000,
                     TaskType::RECURRING );
    // Wait 1 second, timer should be fired 5 times.
    TestSuite::sleep_sec(1);

    // Re-schedule the timer, now interval is 100 ms.
    tt->reschedule(100000);
    TestSuite::_msgt("rescheduled 200 ms -> 100 ms\n");

    // Wait another 1 second, timer should be fired 10 times.
    TestSuite::sleep_sec(1);

    // Shutdown thread pool.
    mgr.shutdown();
    return 0;
}

int mixed_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    // Initialize thread pool.
    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    // Recurring timer whose interval is 200 ms.
    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] recurring 200 ms\n", tid_simple());
                 },
                 200000,
                 TaskType::RECURRING );

    // Recurring timer whose interval is 300 ms.
    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] recurring 300 ms\n", tid_simple());
                 },
                 300000,
                 TaskType::RECURRING );

    // One-time timer whose interval is 500 ms.
    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] one-time 500 ms\n", tid_simple());
                 },
                 500000 );

    std::mutex mm;

    // 40 async tasks.
    for (size_t ii=0; ii<10; ++ii) {
        for (size_t jj=0; jj<4; ++jj) {
            mgr.addTask( [ii, jj, &mm](const TaskResult& ret) {
                std::lock_guard<std::mutex> l(mm);
                TestSuite::_msgt("[%02zx] async task %zu, %zu\n",
                                 tid_simple(), ii, jj);
            } );
        }
        TestSuite::sleep_ms(100);
    }

    // Shutdown thread pool.
    mgr.shutdown();
    return 0;
}

DEFINE_PARAMS_2( unfinished_tasks_test_params,
                 bool, cancel_flag, ({false, true}),
                 size_t, num_threads, ({0, 1, 4}) );

int unfinished_tasks_test(PARAM_BASE) {
    GET_PARAMS(unfinished_tasks_test_params);

    ThreadPoolOptions opt;
    opt.numInitialThreads = unfinished_tasks_test_params->num_threads;
    opt.invokeCanceledTask = unfinished_tasks_test_params->cancel_flag;

    // Initialize thread pool.
    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    std::mutex mm;

    // 15 async tasks.
    for (size_t ii=0; ii<15; ++ii) {
        mgr.addTask( [ii, &mm](const TaskResult& ret) {
            std::lock_guard<std::mutex> l(mm);
            TestSuite::_msgt("[%02zx] async task %zu, result %d\n",
                             tid_simple(), ii, ret);
            if (ret.ok()) {
                TestSuite::sleep_ms(10);
            }
        } );
    }

    // Recurring timer whose interval is 20 ms.
    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] recurring 20 ms, result %d\n",
                                      tid_simple(), ret);
                 },
                 20*1000,
                 TaskType::RECURRING );

    // One-time timer whose interval is 500 ms.
    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] one-time 500 ms, result %d\n",
                                      tid_simple(), ret);
                 },
                 500000 );

    // Wait 50 ms.
    TestSuite::sleep_ms(50);

    // Shutdown thread pool.
    // If `opt.invokeCanceledTask = true`, unfinished tasks will be fired
    // with `CANCELED` result code. Otherwise, they will be just purged.
    TestSuite::_msgt("shutdown thread pool\n");
    mgr.shutdown();

    return 0;
}

int main(int argc, char** argv) {
    TestSuite test(argc, argv);
    test.options.printTestMessage = true;

    test.doTest( "basic test",
                 basic_test,
                 TestRange<size_t>( {0, 1, 4} ) );

    test.doTest( "recurring test",
                 recurring_test,
                 TestRange<size_t>( {0, 1, 2} ) );

    test.doTest( "relay test",
                 relay_test,
                 TestRange<size_t>( {0, 1, 2} ) );

    test.doTest( "fast relay test",
                 fast_relay_test,
                 TestRange<int>( {0, 1, 2} ) );

    test.doTest( "reschedule one time test",
                 reschedule_one_time_test,
                 TestRange<size_t>( {0, 1, 2} ) );

    test.doTest( "reschedule recurring test",
                 reschedule_recurring_test,
                 TestRange<size_t>( {0, 1, 2} ) );

    test.doTest( "mixed test",
                 mixed_test,
                 TestRange<size_t>( {0, 1, 4} ) );

    SET_PARAMS(unfinished_tasks_test_params);
    test.doTest( "unfinished tasks test",
                 unfinished_tasks_test,
                 unfinished_tasks_test_params );

    return 0;
}

