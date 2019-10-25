Simple Thread Pool
==================
Simple and lightweight (single header file) fully asynchronous thread pool and timer.


Author
------
Jung-Sang Ahn <jungsang.ahn@gmail.com>


Build and Run Tests
-------------------
```sh
$ mkdir build
$ cd build
build$ cmake ..
build$ make
build$ ./basic_test
```

How to Use
----------
Include [`src/simple_thread_pool.h`](src/simple_thread_pool.h) in your source code.

[`examples/example.cc`](examples/example.cc):
```C++
using namespace simple_thread_pool;

// Initialize a pool with 4 threads.
ThreadPoolOptions opt;
opt.numInitialThreads = 4;

ThreadPoolMgr mgr;
mgr.init(opt);

// Print hello world asynchronously.
mgr.addTask( [](const TaskResult& ret) { printf("hello world!\n"); } );

// Timer. Print hello world after 1 second (1,000,000 microseconds).
mgr.addTask( [](const TaskResult& ret) { printf("hello world!!\n"); },
             1000000 );

// Recurring timer. Print hello world for every 2 second.
mgr.addTask( [](const TaskResult& ret) { printf("hello world!!!\n"); },
             2000000,
             TaskType::RECURRING );

// Within 3 seconds, all above events will be fired.
std::this_thread::sleep_for( std::chrono::seconds(3) );

// Shutdown.
mgr.shutdown();
```

Please refer to [`tests/basic_test.cc`](tests/basic_test.cc) for more examples.
