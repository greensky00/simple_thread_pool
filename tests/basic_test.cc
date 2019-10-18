#include "simple_thread_pool.h"

#include "test_common.h"

using namespace simple_thread_pool;

size_t tid_simple() {
    return std::hash<std::thread::id>{}(std::this_thread::get_id()) & 0xff;
}

int basic_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    ThreadPoolMgr mgr;
    mgr.init(opt);

    std::mutex mm;
    for (size_t ii=0; ii<20; ++ii) {
        mgr.addTask( [ii, &mm](const TaskResult& ret) {
            std::lock_guard<std::mutex> l(mm);
            TestSuite::_msgt("[%02zx] hello world %d\n",
                             tid_simple(), ii);
        } );
    }

    TestSuite::sleep_sec(1);

    mgr.shutdown();

    return 0;
}

int recurring_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    size_t count = 0;
    std::shared_ptr<TaskHandle> tt =
        mgr.addTask( [&count](const TaskResult& ret) {
                         TestSuite::_msgt("[%02zx] hello world %zu\n",
                                          tid_simple(), count++);
                     },
                     100000,
                     TaskType::RECURRING );
    TestSuite::sleep_sec(1);
    tt->cancel();
    TestSuite::_msgt("canceled timer\n");

    TestSuite::sleep_sec(1);

    mgr.shutdown();

    return 0;
}

void relay_func(size_t* count, ThreadPoolMgr* mgr, const TaskResult& ret) {
    TestSuite::_msgt("[%02zx] hello world %zu\n",
                     tid_simple(), (*count)++);
    if (*count < 10) {
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

    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");
    size_t count = 0;
    relay_func(&count, &mgr, TaskResult());

    TestSuite::sleep_sec(2);

    mgr.shutdown();

    return 0;
}

int reschedule_one_time_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    std::shared_ptr<TaskHandle> tt =
        mgr.addTask( [](const TaskResult& ret) {
                         TestSuite::_msgt("[%02zx] hello world\n",
                                          tid_simple());
                     },
                     5000000 );
    TestSuite::sleep_sec(1);
    tt->reschedule(100000);
    TestSuite::_msgt("rescheduled 5 sec -> 100 ms\n");

    TestSuite::sleep_sec(1);

    mgr.shutdown();

    return 0;
}

int reschedule_recurring_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    size_t count = 0;
    std::shared_ptr<TaskHandle> tt =
        mgr.addTask( [&count](const TaskResult& ret) {
                         TestSuite::_msgt("[%02zx] hello world %zu\n",
                                          tid_simple(), count++);
                     },
                     200000,
                     TaskType::RECURRING );
    TestSuite::sleep_sec(1);
    tt->reschedule(100000);
    TestSuite::_msgt("rescheduled 200 ms -> 100 ms\n");

    TestSuite::sleep_sec(1);

    mgr.shutdown();

    return 0;
}

int mixed_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] recurring 200 ms\n", tid_simple());
                 },
                 200000,
                 TaskType::RECURRING );

    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] recurring 300 ms\n", tid_simple());
                 },
                 300000,
                 TaskType::RECURRING );

    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] one-time 500 ms\n", tid_simple());
                 },
                 500000 );

    std::mutex mm;
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

    mgr.shutdown();

    return 0;
}

int unfinished_tasks_test(size_t num_threads) {
    ThreadPoolOptions opt;
    opt.numInitialThreads = num_threads;

    ThreadPoolMgr mgr;
    mgr.init(opt);

    TestSuite::_msgt("begin\n");

    std::mutex mm;
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

    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] recurring 20 ms, result %d\n",
                                      tid_simple(), ret);
                 },
                 20*1000,
                 TaskType::RECURRING );

    mgr.addTask( [](const TaskResult& ret) {
                     TestSuite::_msgt("[%02zx] one-time 500 ms, result %d\n",
                                      tid_simple(), ret);
                 },
                 500000 );

    TestSuite::sleep_ms(50);

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

    test.doTest( "reschedule one time test",
                 reschedule_one_time_test,
                 TestRange<size_t>( {0, 1, 2} ) );

    test.doTest( "reschedule recurring test",
                 reschedule_recurring_test,
                 TestRange<size_t>( {0, 1, 2} ) );

    test.doTest( "mixed test",
                 mixed_test,
                 TestRange<size_t>( {0, 1, 4} ) );

    test.doTest( "unfinished tasks test",
                 unfinished_tasks_test,
                 TestRange<size_t>( {0, 1, 4} ) );

    return 0;
}

