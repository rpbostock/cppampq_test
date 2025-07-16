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

std::thread TestAmqp::forceCloseConnections(std::atomic<bool>& finish, const std::chrono::milliseconds& interval)
{
	std::atomic<int> rc(0);
	std::thread forceClose([&interval, &finish, &rc]() {
		while (finish.load() == false)
		{
			std::this_thread::sleep_for(interval);
			LOG_INFO("Forcing close of connections by timer interval after delay of " << interval.count() << "ms");
			rc = forceCloseConnections_();
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

int TestAmqp::forceCloseConnections_()
{
	LOG_INFO("Disconnecting all connections");
	return system("rabbitmqadmin -f tsv -q list connections name | cut -f1 | xargs -I {} rabbitmqadmin -q close connection name={}");
}




TEST_F(TestAmqp, testTransmitChannel_short	)
{
	GTEST_LOG_(INFO) << "Test that we can send some messages successfully to an exchange - no feedback at this point";
	testTransmitChannel_(100);
}

TEST_F(TestAmqp, testTransmitChannel_long	)
{
	GTEST_LOG_(INFO) << "Test that we can send 1M messages successfully to an exchange - no feedback at this point";
	testTransmitChannel_(1E6);
}



void TestAmqp::testTransmitChannel_(const size_t num_messages)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	rmq::ChannelConfig config {"testTransmitChannel_short_exchange", "testTransmitChannel_short_queue", "testTransmitChannel_short_routing"};
	auto channel_listener = std::make_shared<rmq::ChannelListener>();
	controller.createTransmitChannel(config, channel_listener);
	controller.start();

	// Ensure we're up and running
	auto start = std::chrono::high_resolution_clock::now();
	while ((!controller.isConnectionReady()	|| !channel_listener->isActive())
		&& std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(controller.isConnectionReady());
	GTEST_ASSERT_TRUE(channel_listener->isActive());

	// Send some messages
	const auto queue = controller.getTxQueue(config.exchange_name);
	GTEST_ASSERT_TRUE(queue != nullptr);

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
		&& channel_listener->getNumberOfTransmittedMessages() == num_messages
		&& send_complete.load())
		&& std::chrono::high_resolution_clock::now() - start < getTransmitTimeout_(num_messages))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(queue->isEmpty());
	GTEST_ASSERT_TRUE(send_complete.load());
	GTEST_ASSERT_EQ(channel_listener->getNumberOfTransmittedMessages(), num_messages);

}

std::chrono::seconds TestAmqp::getTransmitTimeout_(const size_t num_messages)
{
	return std::chrono::seconds(static_cast<int>( ceil(num_messages / 5000.0)));
}

std::chrono::seconds TestAmqp::getReceiveTimeout_(const size_t num_messages)
{
	return std::chrono::seconds(static_cast<int>( ceil(num_messages / 1000.0)));
}




TEST_F(TestAmqp, testReconnectionTxChannel_short)
{
	GTEST_LOG_(INFO) << "Test that we can send 100k messages successfully to an exchange - no feedback at this point";
	testTransmitChannelWithReconnect_(1000000);
}

void TestAmqp::testTransmitChannelWithReconnect_(const size_t num_messages)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	rmq::ChannelConfig config {"testTransmitChannel_short_exchange", "testTransmitChannel_short_queue", "testTransmitChannel_short_routing"};
	auto channel_listener = std::make_shared<rmq::ChannelListener>();
	controller.createTransmitChannel(config, channel_listener);
	controller.start();

	// Ensure we're up and running
	auto start = std::chrono::high_resolution_clock::now();
	while (!(controller.isConnectionReady()	&& channel_listener->isActive())
		&& std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(controller.isConnectionReady());
	GTEST_ASSERT_TRUE(channel_listener->isActive());

	// // Ensure at least one reconnection occurs
	// GTEST_ASSERT_EQ(forceCloseConnections_(), 0);
	// std::this_thread::sleep_for(std::chrono::seconds(1));

	// Send some messages
	const auto queue = controller.getTxQueue(config.exchange_name);
	std::atomic send_complete(false);
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

	std::atomic<bool> finish(false);
	auto interval = std::chrono::milliseconds(5000);
	auto forceDisconnectThread = forceCloseConnections(finish, interval);

	// Wait for them all to be sent
	start = std::chrono::high_resolution_clock::now();
	while (!(queue->isEmpty()
		&& channel_listener->getNumberOfTransmittedMessages() == num_messages
		&& send_complete.load())
		&& std::chrono::high_resolution_clock::now() - start < getTransmitTimeout_(num_messages)*2)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(queue->isEmpty());
	GTEST_ASSERT_TRUE(send_complete.load());
	GTEST_ASSERT_EQ(channel_listener->getNumberOfTransmittedMessages(), num_messages);

	finish.store(true);
	forceDisconnectThread.join();

}

TEST_F(TestAmqp, testReceiveChannel_short)
{
	constexpr size_t num_messages = 100;
	GTEST_LOG_(INFO) << "Test that we can receive " << num_messages << " messages successfully on a queue";
	testReceiveChannelAsync_(num_messages);
}

TEST_F(TestAmqp, testReceiveChannel_long)
{
	constexpr size_t num_messages = 1E6;
	GTEST_LOG_(INFO) << "Test that we can receive " << num_messages << " messages successfully on a queue";
	testReceiveChannelAsync_(num_messages);
}

void TestAmqp::testReceiveChannelAsync_(const size_t num_messages)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	rmq::ChannelConfig config {"testTransmitChannel_short_exchange", "testTransmitChannel_short_queue", "testTransmitChannel_short_routing"};
	config.qos_prefetch_count = 200;
	const auto rx_channel_listener = std::make_shared<rmq::ChannelListener>();
	const auto tx_channel_listener = std::make_shared<rmq::ChannelListener>();
	const std::string rx_channel_name = controller.createReceiveChannel(config, rx_channel_listener);
	const std::string tx_channel_name = controller.createTransmitChannel(config, tx_channel_listener);
	controller.start();

	// Ensure we're up and running
	auto start = std::chrono::high_resolution_clock::now();
	while (!(controller.isConnectionReady()
		&& tx_channel_listener->isActive()
		&& rx_channel_listener->isActive())
		&& std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(controller.isConnectionReady());
	GTEST_ASSERT_TRUE(tx_channel_listener->isActive());
	GTEST_ASSERT_TRUE(rx_channel_listener->isActive());

	// Send some messages
	const auto tx_queue = controller.getTxQueue(tx_channel_name);
	GTEST_ASSERT_TRUE(tx_queue != nullptr);
	std::jthread send_thread([&tx_queue, num_messages]()
	{
		for (size_t i=0; i<num_messages; i++)
		{
			std::string message = "test message " + std::to_string(i);
			auto message_vec = std::make_shared<std::vector<char> >(message.begin(), message.end());
			tx_queue->push(message_vec);
		}
	});

	// Receive the messages
	const auto rx_queue = controller.getRxQueue(rx_channel_name);
	GTEST_ASSERT_TRUE(rx_queue != nullptr);
	std::atomic<size_t> received_messages {0};
	std::atomic<bool> finish{false};
	std::jthread receive_thread([&rx_channel_name, &controller, &finish, &rx_queue, &received_messages]()
	{
		while (!finish.load())
		{
			if (!rx_queue->isEmpty())
			{
				auto message = rx_queue->pop();
				++received_messages;
				controller.acknowledge(rx_channel_name, message.getAck()); // TODO Need to use a second queue for acks?
				if (received_messages%10000 == 0)
				{
					LOG_INFO("Received message " << received_messages);
				}
			}
			else
			{
				LOG_TRACE("Queue is empty");
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
		}
	});

	start = std::chrono::high_resolution_clock::now();
	auto timeout = std::max(getTransmitTimeout_(num_messages), getReceiveTimeout_(num_messages));
	LOG_INFO("Waiting for all the messages to have been transmitted and received: " << timeout.count() << " seconds");
	while (received_messages != num_messages
		&& std::chrono::high_resolution_clock::now() - start < timeout
		)
	{

		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	finish.store(true);
	receive_thread.join();
	send_thread.join();
	GTEST_ASSERT_EQ(received_messages, num_messages);

}

TEST_F(TestAmqp, testTxRxMultipleSeparateChannels_short)
{
	constexpr size_t num_messages = 100;
	constexpr size_t num_channels = 2;
	GTEST_LOG_(INFO) << "Test that we can receive " << num_messages << " messages successfully on " << num_channels << " channels in parallel";
	testMultipleTxRxChannelsAsync_(num_messages, num_channels);

	GTEST_LOG_(INFO) << "Test that we can receive " << num_messages << " messages successfully on " << num_channels*5 << " channels in parallel";
	testMultipleTxRxChannelsAsync_(num_messages, num_channels*5);
}

TEST_F(TestAmqp, testTxRxMultipleSeparateChannels_long)
{
	constexpr size_t num_messages = 1E6;
	constexpr size_t num_channels = 2;
	GTEST_LOG_(INFO) << "Test that we can receive " << num_messages << " messages successfully on " << num_channels << " channels in parallel";
	testMultipleTxRxChannelsAsync_(num_messages, num_channels);
}




void TestAmqp::testMultipleTxRxChannelsAsync_(const size_t num_messages, const size_t num_channels)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	controller.start();

	// Separate test threads
	std::set<std::shared_ptr<TxRxThreadEntry>> entries;
	for (auto i=0; i<num_channels; i++)
	{
		rmq::ChannelConfig config {"testMultipleTxRxChannelsAsync_exchange_" + std::to_string(i)
			, "testMultipleTxRxChannelsAsync_exchange_queue_" + std::to_string(i)
			, "testMultipleTxRxChannelsAsync_routing_" + std::to_string(i)};
		entries.emplace(std::make_shared<TxRxThreadEntry>(config));
	}

	for (auto& entry : entries)
	{
		testSingleTxRxChannelsAsync_(controller, entry, num_messages);
	}

	// Check that all of the threads have now finished...
	while (!std::ranges::all_of(entries, [](const auto &entry) { return entry->getFinish().load(); }))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}


}

void TestAmqp::testSingleTxRxChannelsAsync_(rmq::MyAmqpController &controller
	, const std::shared_ptr<TxRxThreadEntry>& entry
	, size_t num_messages)
{
	auto config = entry->getChannelConfig();
	config.qos_prefetch_count = 200;
	auto &finish = entry->getFinish();
	const auto test_thread = std::make_shared<std::jthread>([&controller, config, &finish, num_messages]()
		{
			const auto rx_channel_listener = std::make_shared<rmq::ChannelListener>();
			const auto tx_channel_listener = std::make_shared<rmq::ChannelListener>();
			const std::string rx_channel_name = controller.createReceiveChannel(config, rx_channel_listener);
			const std::string tx_channel_name = controller.createTransmitChannel(config, tx_channel_listener);

			// Ensure we're up and running
			auto start = std::chrono::high_resolution_clock::now();
			while (!(controller.isConnectionReady()
			         && tx_channel_listener->isActive()
			         && rx_channel_listener->isActive())
			       && std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			GTEST_ASSERT_TRUE(controller.isConnectionReady()) << "Connection is not ready on " << config.exchange_name;
			GTEST_ASSERT_TRUE(tx_channel_listener->isActive()) << "Channel is not active on " << tx_channel_name;
			GTEST_ASSERT_TRUE(rx_channel_listener->isActive()) << "Channel is not active on " << rx_channel_name;

			// Send some messages
			const auto tx_queue = controller.getTxQueue(tx_channel_name);
			GTEST_ASSERT_TRUE(tx_queue != nullptr);
			std::jthread send_thread([&tx_queue, num_messages]()
			{
				for (size_t i = 0; i < num_messages; i++)
				{
					std::string message = "test message " + std::to_string(i);
					auto message_vec = std::make_shared<std::vector<char> >(message.begin(), message.end());
					tx_queue->push(message_vec);
				}
			});

			// Receive the messages
			const auto rx_queue = controller.getRxQueue(rx_channel_name);
			GTEST_ASSERT_TRUE(rx_queue != nullptr) << "Cannot obtain queue for " << rx_channel_name;
			std::atomic<size_t> received_messages{0};
			std::jthread receive_thread([&rx_channel_name, &controller, &finish, &rx_queue, &received_messages]()
			{
				auto ack_fn = controller.getAckFunction(rx_channel_name);
				while (!finish.load())
				{
					if (!rx_queue->isEmpty())
					{
						auto message = rx_queue->pop();
						++received_messages;
						ack_fn(message.getAck());
						// controller.acknowledge(rx_channel_name, message.getAck());
						// TODO Need to use a second queue for acks?
						if (received_messages % 10000 == 0)
						{
							LOG_INFO("Received message " << received_messages);
						}
					}
					else
					{
						LOG_TRACE("Queue is empty");
						std::this_thread::sleep_for(std::chrono::milliseconds(100));
					}
				}
			});

			start = std::chrono::high_resolution_clock::now();
			auto timeout = std::max(getTransmitTimeout_(num_messages), getReceiveTimeout_(num_messages));
			LOG_INFO(
				"Waiting for all the messages to have been transmitted and received: " << timeout.count() << " seconds")
			;
			while (received_messages != num_messages
			       && std::chrono::high_resolution_clock::now() - start < timeout
			)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			finish.store(true);
			receive_thread.join();
			send_thread.join();
			GTEST_ASSERT_EQ(received_messages, num_messages) << "Received " << received_messages << " messages on " << rx_channel_name << " instead of " << num_messages;
		}
	);
	entry->setTestThread(test_thread);
}

