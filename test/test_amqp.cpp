#include "test_amqp.hpp"
#include "my_amqp_controller_example.hpp"
#include "my_amqp_controller_no_channel.hpp"
#include "my_amqp_controller.hpp"
#include "my_amqp_controller_single_thread.hpp"


TEST_F(TestAmqp, testStartStopExampleWithSingleChannel_short)
{
	GTEST_LOG_(INFO) << "Start and stop one channel a thousand times.";
	GTEST_ASSERT_TRUE(testStartStopExampleWithSingleChannel_(1, 1));
	GTEST_ASSERT_TRUE(testStartStopExampleWithSingleChannel_(100, 10));
}

bool TestAmqp::testStartStopExampleWithSingleChannel_(int num_repeats, int num_threads)
{
	for (int i = 0; i < num_repeats; i++)
	{
		std::vector<std::thread> myThreads(num_threads);
		for (auto &thread : myThreads)
		{
			thread = std::thread([]() {
				MyAmqpControllerExample controller;
				controller.run();
			});
		}

		for (auto &thread : myThreads)
		{
			thread.join();
		}
	}
	return true;
}

TEST_F(TestAmqp, testStartStopExampleWithNoChannel_short)
{
	GTEST_LOG_(INFO) << "Start and stop without a channel a thousand times.";
	GTEST_ASSERT_TRUE(testStartStopExampleWithNoChannel_(10000, 10));
	// GTEST_ASSERT_TRUE(testStartStopExampleWithNoChannel_(10000, 10));
	// GTEST_ASSERT_TRUE(testStartStopExampleWithNoChannel_(1000, 10));
}

bool TestAmqp::testStartStopExampleWithNoChannel_(int num_repeats, int num_threads)
{
	for (int repeat = 0; repeat < num_repeats; repeat++)
	{
		std::vector<std::thread> myThreads(num_threads);
		std::vector<MyAmqpControllerNoChannel> controllers(num_threads);
		for (auto t=0; t<num_threads; t++)
		{
			auto &controller = controllers[t];
			myThreads[t] = std::thread([&controller]() {
				controller.run();
			});
		}

		while (!std::ranges::all_of(controllers, [](auto &entry) { return entry.isConnectionReady(); }))
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}

		for (auto &controller : controllers)
		{
			controller.close();
		}

		while (std::ranges::any_of(controllers, [](auto &entry) { return entry.isRequestClose(); }))
		{
			std::ranges::for_each(controllers, [](auto &entry) { entry.nudge_event_loop(); });
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		std::ranges::for_each(controllers, [](auto &entry) { entry.nudge_event_loop(); });

		for (auto &thread : myThreads)
		{
			thread.join();
		}
	}
	return true;
}

TEST_F(TestAmqp, testStartStopRealNoChannel_short)
{
	GTEST_LOG_(INFO) << "Start and stop without a channel once.";
	GTEST_ASSERT_TRUE(testStartStopRealNoChannel_(1000, 1));
}

TEST_F(TestAmqp, testStartStopRealNoChannel_long)
{
	GTEST_LOG_(INFO) << "Start and stop without a channel a thousand times.";
	GTEST_ASSERT_TRUE(testStartStopRealNoChannel_(1000, 10));
}

bool TestAmqp::testStartStopRealNoChannel_(int num_repeats, int num_threads)
{
	// TODO - Need to make this so that the threads are genuinely handling multiple at the same time.

	for (int i = 0; i < num_repeats; i++)
	{
		std::cout << "Starting repeat " << i << std::endl;
		std::vector<std::thread> myThreads(num_threads);
		for (auto &thread : myThreads)
		{
			thread = std::thread([]()
			{
				rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
				controller.start();
				while (!controller.isConnectionReady())
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
			});

			// std::cout << "Connection is now ready - should be deleting the controller" << std::endl;
		}

		for (auto &thread : myThreads)
		{
			thread.join();
		}
	}
	return true;
}

TEST_F(TestAmqp, testReconnectionNoChannel_short)
{
	GTEST_LOG_(INFO) << "Start and stop without a channel a thousand times.";
	GTEST_ASSERT_TRUE(testForceReconnectNoChannel_(1, 1));
}

TEST_F(TestAmqp, testReconnectionNoChannel_long)
{
	GTEST_LOG_(INFO) << "Start and stop without a channel a thousand times.";
	GTEST_ASSERT_TRUE(testForceReconnectNoChannel_(10, 10));
}

bool TestAmqp::testForceReconnectNoChannel_(int num_repeats, int num_threads)
{
	std::atomic<bool> enough_reconnections(true);
	for (int i = 0; i < num_repeats; i++)
	{
		std::cout << "Starting repeat " << i << std::endl;
		std::atomic<bool> finish(false);
		std::vector<std::thread> myThreads(num_threads);
		for (auto &thread : myThreads)
		{
			thread = std::thread([&finish, &enough_reconnections]()
			{
				rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
				controller.start();
				while (finish.load() == false)
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(100));
				}
				if (const auto reconnections = controller.getNumReconnections(); reconnections < 5)
				{
					std::cout << "Not enough reconnections - only had " << reconnections << std::endl;
					enough_reconnections.store(false);
				};
			});

			// std::cout << "Connection is now ready - should be deleting the controller" << std::endl;
		}

		// Small delay until we start forcing restarts on the connections
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		auto interval = std::chrono::milliseconds(5000);
		auto forceDisconnectThread = forceCloseConnections(finish, interval);
		std::this_thread::sleep_for(std::chrono::seconds(60));
		std::cout << "Finishing test off" << std::endl;
		finish.store(true);

		for (auto &thread : myThreads)
		{
			thread.join();
		}
		forceDisconnectThread.join();
	}

	return enough_reconnections.load();
}

std::thread TestAmqp::forceCloseConnections(std::atomic<bool>& finish, std::chrono::milliseconds& interval)
{
	std::atomic<int> rc(0);
	std::thread forceClose([&interval, &finish, &rc]() {
		while (finish.load() == false)
		{
			std::this_thread::sleep_for(interval);
			rc = system("rabbitmqadmin -f tsv -q list connections name | cut -f1 | xargs -I {} rabbitmqadmin -q close connection name={}");
			if (rc == 0)
			{
				GTEST_LOG_(INFO) << "All connections closed.";
			}
			else
			{
				break;
			}
		}
	});
	return forceClose;
}


TEST_F(TestAmqp, testStartStopSTWithNoChannel_short)
{
	GTEST_LOG_(INFO) << "Start and stop without a channel a thousand times.";
	GTEST_ASSERT_TRUE(testStartStopSTWithNoChannel2_(1, 1));
	GTEST_ASSERT_TRUE(testStartStopSTWithNoChannel2_(1000, 10));
	// GTEST_ASSERT_TRUE(testStartStopSTWithNoChannel_(1, 1));
	// GTEST_ASSERT_TRUE(testStartStopSTWithNoChannel_(1000, 10));
}

bool TestAmqp::testStartStopSTWithNoChannel_(int num_repeats, int num_threads)
{
	for (int i = 0; i < num_repeats; i++)
	{
		std::cout << "Starting repeat " << i << std::endl;
		std::vector<std::thread> myThreads(num_threads);
		for (auto &thread : myThreads)
		{
			thread = std::thread([]() {
				MyAmqpControllerSingleThread controller;
				controller.run();
			});
		}

		for (auto &thread : myThreads)
		{
			thread.join();
		}
	}
	return true;
}

bool TestAmqp::testStartStopSTWithNoChannel2_(int num_repeats, int num_threads)
{
	event_enable_debug_mode();

	for (int repeat = 0; repeat < num_repeats; repeat++)
	{
		std::vector<std::thread> myThreads(num_threads);
		std::vector<MyAmqpControllerSingleThread> controllers(num_threads);
		for (auto t=0; t<num_threads; t++)
		{
			auto &controller = controllers[t];
			myThreads[t] = std::thread([&controller]() {
				controller.run();
			});
		}

		while (!std::ranges::all_of(controllers, [](auto &entry) { return entry.isConnectionReady(); }))
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}

		for (auto &controller : controllers)
		{
			controller.triggerCloseEvent();
			controller.notifyEventLoop();
		}

		for (auto &thread : myThreads)
		{
			thread.join();
		}
	}
	return true;
}