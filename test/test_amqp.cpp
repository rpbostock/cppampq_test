#include "test_amqp.hpp"
#include "my_amqp_controller_example.hpp"
#include "my_amqp_controller_no_channel.hpp"
#include "my_amqp_controller.hpp"

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
	GTEST_ASSERT_TRUE(testStartStopExampleWithNoChannel_(1, 1));
	GTEST_ASSERT_TRUE(testStartStopExampleWithNoChannel_(1000, 10));
}

bool TestAmqp::testStartStopExampleWithNoChannel_(int num_repeats, int num_threads)
{
	for (int i = 0; i < num_repeats; i++)
	{
		std::vector<std::thread> myThreads(num_threads);
		for (auto &thread : myThreads)
		{
			thread = std::thread([]() {
				MyAmqpControllerNoChannel controller;
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

TEST_F(TestAmqp, testStartStopRealNoChannel_short)
{
	GTEST_LOG_(INFO) << "Start and stop without a channel once.";
	GTEST_ASSERT_TRUE(testStartStopRealNoChannel_(1, 1));
}

TEST_F(TestAmqp, testStartStopRealNoChannel_long)
{
	GTEST_LOG_(INFO) << "Start and stop without a channel a thousand times.";
	GTEST_ASSERT_TRUE(testStartStopRealNoChannel_(100, 10));
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
	GTEST_LOG_(INFO) << "Test a single set of reconnections over a minute with a single AMQP connection";
	GTEST_ASSERT_TRUE(testForceReconnectNoChannel_(1, 1));
}

TEST_F(TestAmqp, testReconnectionNoChannel_long)
{
	GTEST_LOG_(INFO) << "Test multiple sets of reconnections over a 10 minute period with a multiple AMQP connection";
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



TEST_F(TestAmqp, testTransmitChannel_short_	)
{
	GTEST_LOG_(INFO) << "Test that we can send some messages successfully to an exchange - no feedback at this point";
	testTransmitChannel_(100);
}

TEST_F(TestAmqp, testTransmitChannel_long_	)
{
	GTEST_LOG_(INFO) << "Test that we can send 1M messages successfully to an exchange - no feedback at this point";
	testTransmitChannel_(1E6);
}



void TestAmqp::testTransmitChannel_(const size_t num_messages)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	rmq::ChannelConfig config {"testTransmitChannel_short_exchange, testTransmitChannel_short_queue, testTransmitChannel_short_routing"};
	auto channel = controller.createTransmitChannel(config);
	controller.start();

	// Ensure we're up and running
	auto start = std::chrono::high_resolution_clock::now();
	while ((!controller.isConnectionReady()	|| !channel->isActive())
		&& std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(controller.isConnectionReady());
	GTEST_ASSERT_TRUE(channel->isActive());

	// Send some messages
	const auto queue = channel->getHandler();
	std::atomic<bool> send_complete(false);
	std::jthread send_thread([&queue, &send_complete, num_messages]()
	{
		for (size_t i=0; i<num_messages; i++)
		{
			std::string message = "test message " + std::to_string(i);
			auto message_vec = std::make_shared<std::vector<char> >(message.begin(), message.end());
			queue->push(message_vec);
		}
		send_complete.store(true);
	});

	// Wait for them all to be sent
	start = std::chrono::high_resolution_clock::now();
	while (!(queue->isEmpty()
		&& channel->getNumberOfTransmittedMessages() == num_messages
		&& send_complete.load())
		&& std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(static_cast<int>( ceil(num_messages / 5000.0))))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(queue->isEmpty());
	GTEST_ASSERT_TRUE(send_complete.load());
	GTEST_ASSERT_EQ(channel->getNumberOfTransmittedMessages(), num_messages);

}
