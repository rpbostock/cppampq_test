#include "test_amqp.hpp"
#include "my_amqp_controller_example.hpp"
#include "my_amqp_controller_no_channel.hpp"
#include "my_amqp_controller.hpp"
#include "rx_client_wrapper.hpp"
#include "test_reliable_message_manager.hpp"
#include "tx_client_wrapper.hpp"

using namespace rmq;

TEST_F(TestAmqp, testStartStopExampleWithSingleChannel_short)
{
	GTEST_LOG_(INFO) << "Start and stop one channel a thousand times.";
	GTEST_ASSERT_TRUE(testStartStopExampleWithSingleChannel_(1, 1));
	GTEST_ASSERT_TRUE(testStartStopExampleWithSingleChannel_(10000, 10));
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
	// GTEST_ASSERT_TRUE(testStartStopExampleWithNoChannel_(1, 1));
	GTEST_ASSERT_TRUE(testStartStopExampleWithNoChannel_(10000, 10));
}

bool TestAmqp::testStartStopExampleWithNoChannel_(int num_repeats, int num_threads)
{
	for (int repeat = 0; repeat < num_repeats; repeat++)
	{
		std::vector<std::thread> myThreads;
		myThreads.reserve(num_threads);
		std::vector<std::unique_ptr<MyAmqpControllerNoChannel>> controllers;
		controllers.reserve(num_threads);
		for (auto t=0; t<num_threads; t++)
		{
			controllers.push_back(std::make_unique<MyAmqpControllerNoChannel>());
			auto &controller = controllers.back();
			myThreads.emplace_back(std::thread([&controller]() {
				controller->run();
			}));
		}

		while (!std::ranges::all_of(controllers, [](auto &entry) { return entry->isConnectionReady(); }))
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}

		for (auto &controller : controllers)
		{
			controller->triggerCloseEvent();
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
	GTEST_ASSERT_TRUE(testStartStopRealNoChannel_(10000, 10));
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
	class ForceReconnectThreadWrapper
	{
	public:
		std::thread thread;
		int num_reconnections = 0;
	};

	for (int i = 0; i < num_repeats; i++)
	{
		LOG_DEBUG("Starting repeat " << i);
		std::atomic finish(false);
		std::vector<ForceReconnectThreadWrapper> thread_wrappers(num_threads);
		for (auto &thread_wrapper : thread_wrappers)
		{
			thread_wrapper.thread = std::thread([&finish, &thread_wrapper]()
			{
				rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
				controller.start();
				while (finish.load() == false)
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(100));
				}
				thread_wrapper.num_reconnections = controller.getNumReconnections();
			});
		}

		// Small delay until we start forcing restarts on the connections
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		auto interval = std::chrono::milliseconds(5000);
		int num_forced_reconnections = 0;
		auto forceDisconnectThread = forceCloseConnections(finish, interval, num_forced_reconnections);
		std::this_thread::sleep_for(std::chrono::seconds(60));
		LOG_DEBUG("Finishing test off");
		finish.store(true);

		for (auto &thread : thread_wrappers)
		{
			thread.thread.join();
		}
		forceDisconnectThread.join();

		LOG_DEBUG("Number of forced disconnects: " << num_forced_reconnections);
		if (std::ranges::any_of(thread_wrappers, [&num_forced_reconnections](const auto &wrapper) { return abs(wrapper.num_reconnections - num_forced_reconnections) > 1; }))
		{
			LOG_INFO("Not all threads within 1 reconnections on repeat: " << i << ". Expected reconnections between " << num_forced_reconnections -1 << " and " << num_forced_reconnections + 1);
			for (auto &[thread, num_reconnections] : thread_wrappers)
			{
				LOG_INFO("Wrapper reconnections " << num_reconnections << " on repeat " << i);
			}

			return false;
		}
	}

	return true;
}

std::jthread TestAmqp::forceCloseConnections(std::atomic<bool>& finish, const std::chrono::milliseconds& interval, int& num_forced_reconnections)
{
	std::atomic<int> rc(0);
	std::jthread forceClose([&interval, &finish, &rc, &num_forced_reconnections]() {
		while (finish.load() == false)
		{
			std::this_thread::sleep_for(interval);
			LOG_INFO("Forcing close of connections by timer interval after delay of " << interval.count() << "ms");
			rc = forceCloseConnections_();
				if (rc == 0)
			{
				++num_forced_reconnections;
				LOG_INFO("All connections closed. Forced disconnects: " << num_forced_reconnections);
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
	constexpr size_t num_messages = 100;
	constexpr size_t num_channels = 1;
	GTEST_LOG_(INFO) << "Test that we can send " << num_messages << " messages successfully on " << num_channels << " channels";
	testTransmitChannelWithManager_(num_messages, num_channels);
}

TEST_F(TestAmqp, testTransmitChannel_long)
{
	constexpr size_t num_messages = 1E6;
	constexpr size_t num_channels = 1;
	GTEST_LOG_(INFO) << "Test that we can send " << num_messages << " messages successfully on " << num_channels << " channels";
	testTransmitChannelWithManager_(num_messages, num_channels);
}

TEST_F(TestAmqp, testTransmitMultipleChannels_short	)
{
	constexpr size_t num_messages = 100;
	constexpr size_t num_channels = 10;
	GTEST_LOG_(INFO) << "Test that we can send " << num_messages << " messages successfully on " << num_channels << " channels";
	testTransmitChannelWithManager_(num_messages, num_channels);
}

TEST_F(TestAmqp, testTransmitMultipleChannels_long)
{
	constexpr size_t num_messages = 1E6;
	constexpr size_t num_channels = 10;
	GTEST_LOG_(INFO) << "Test that we can send " << num_messages << " messages successfully on " << num_channels << " channels";
	testTransmitChannelWithManager_(num_messages, num_channels);
}

void TestAmqp::testTransmitChannelWithManager_(const size_t num_messages, const int num_channels)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	controller.setMaxTransmitBatchSize(10000); // We're only interested in transmission so we can put this very high

	std::vector<TxClientWrapper> transmitters;
	for (auto i=0; i<num_channels; i++)
	{
		rmq::ChannelConfig config {"testTransmitChannel_exchange" + std::to_string(i)
			, ""
			, ""};
		const auto channel_listener = std::make_shared<TestReliableMessageManager>(num_messages);
		transmitters.emplace_back(controller.createTransmitChannel(config, channel_listener));
	}
	controller.start();

	// Ensure we're up and running
	auto start = std::chrono::high_resolution_clock::now();
	while (!(controller.isConnectionReady()	&& std::ranges::all_of(transmitters,[](const auto &entry){ return entry.getListener()->isActive(); }))
		&& std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(controller.isConnectionReady());
	GTEST_ASSERT_TRUE(std::ranges::all_of(transmitters,[](const auto &entry){ return entry.getListener()->isActive(); }));

	// Send some messages
	GTEST_ASSERT_TRUE(std::ranges::all_of(transmitters, [](const auto &entry) { return entry.getQueue() != nullptr; }));

	std::atomic send_complete(false);
	std::jthread send_thread = send_data(transmitters, send_complete, num_messages);

	// Wait for them all to be sent
	// std::this_thread::sleep_for(std::chrono::seconds(1000));
	start = std::chrono::high_resolution_clock::now();
	while ( !(std::ranges::all_of(transmitters, [num_messages](const auto &entry) { return entry.getListener()->getNumberOfAcknowledgedMessages() == num_messages; })
		&& send_complete.load())
		&& std::chrono::high_resolution_clock::now() - start < getTransmitTimeout_(num_messages) * num_channels)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_LE(std::chrono::high_resolution_clock::now() - start, getTransmitTimeout_(num_messages) * num_channels) << "Timeout waiting for all data to be transmitted";

	for (auto entry : transmitters)
	{
		LOG_INFO("number of acknowledged messages: " << entry.getListener()->getNumberOfAcknowledgedMessages());
	}

	GTEST_ASSERT_TRUE(std::ranges::all_of(transmitters, [](const auto &entry) { return entry.getQueue()->isEmpty(); }));
	GTEST_ASSERT_TRUE(std::ranges::all_of(transmitters, [num_messages](const auto &entry) { return entry.getListener()->getNumberOfAcknowledgedMessages() == num_messages; }));
	GTEST_ASSERT_TRUE(send_complete.load());

}

std::jthread TestAmqp::send_data(std::vector<rmq::TxClientWrapper> &transmitters, std::atomic<bool> &send_complete,
	int num_messages)
{
	return std::jthread([&transmitters, &send_complete, num_messages]()
	{
		bool new_data = true;
		bool max_unacked_reached = false;
		constexpr size_t max_unacked = 1000;
		while (new_data || max_unacked_reached)
		{
			new_data = false;
			max_unacked_reached = false;

			for ( auto entry : transmitters)
			{
				auto reliable_message_manager = std::dynamic_pointer_cast<TestReliableMessageManager>(entry.getListener());
				if (reliable_message_manager != nullptr && !reliable_message_manager->isEmpty() && reliable_message_manager->numUnacknowledged() < max_unacked)
				{
					auto message_vec = reliable_message_manager->getNextMessage();
					std::string message = std::string(message_vec->begin(), message_vec->end());
					LOG_DEBUG("Sending message " << message << " num unacked: " << reliable_message_manager->numUnacknowledged() << " num unsent " << reliable_message_manager->numUnsent());
					entry.getQueue()->push(message_vec);
					new_data = true;
				}
				if (reliable_message_manager->numUnacknowledged() >= max_unacked)
				{
					max_unacked_reached = true;
				}
			}
			if (max_unacked_reached && !new_data)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
		}
		LOG_INFO("Completed adding " << num_messages << " messages");
		send_complete.store(true);
	});
}

std::chrono::seconds TestAmqp::getTransmitTimeout_(const size_t num_messages)
{
	return std::chrono::seconds(static_cast<int>( ceil(num_messages / 1000.0)));
}

std::chrono::seconds TestAmqp::getReceiveTimeout_(const size_t num_messages)
{
	return std::chrono::seconds(static_cast<int>( ceil(num_messages / 1000.0)));
}


TEST_F(TestAmqp, testReconnectionTxChannel_short)
{
	constexpr size_t num_messages = 2E5;
	GTEST_LOG_(INFO) << "Test that we can send " << num_messages << " messages successfully to an exchange under reconnect conditions - no feedback at this point";
	testTransmitChannelWithReconnect_(num_messages);
}

TEST_F(TestAmqp, testReconnectionTxChannel_long)
{
	constexpr size_t num_messages = 1E7;
	GTEST_LOG_(INFO) << "Test that we can send " << num_messages << " messages successfully to an exchange under reconnect conditions - no feedback at this point";
	testTransmitChannelWithReconnect_(num_messages);
}

void TestAmqp::testTransmitChannelWithReconnect_(const size_t num_messages)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	controller.setMaxTransmitBatchSize(1000); // We're only interested in transmission so we can put this high
	rmq::ChannelConfig config {"testTransmitChannelWithReconnect_exchange"
		, "testTransmitChannelWithReconnect_queue"
		, "testTransmitChannelWithReconnect_routing"};
	std::vector<TxClientWrapper> transmitters;
	transmitters.emplace_back(controller.createTransmitChannel(config
		, std::make_shared<TestReliableMessageManager>(num_messages)));
	auto wrapper = transmitters[0];
	auto listener = wrapper.getListener();
	controller.start();

	// Ensure we're up and running
	auto start = std::chrono::high_resolution_clock::now();
	while (!(controller.isConnectionReady()	&& listener->isActive())
		&& std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(controller.isConnectionReady());
	GTEST_ASSERT_TRUE( wrapper.getListener()->isActive());

	// Send some messages
	const auto queue = wrapper.getQueue();
	std::atomic send_complete(false);
	std::jthread send_thread = send_data(transmitters, send_complete, num_messages);

	std::atomic<bool> finish(false);
	auto interval = std::chrono::milliseconds(20000);
	int num_forced_reconnections = 0;
	auto forceDisconnectThread = forceCloseConnections(finish, interval, num_forced_reconnections);

	// Wait for them all to be sent
	start = std::chrono::high_resolution_clock::now();
	while (!(queue->isEmpty()
		&& listener->getNumberOfAcknowledgedMessages() == num_messages
		&& send_complete.load())
		&& std::chrono::high_resolution_clock::now() - start < getTransmitTimeout_(num_messages)*2)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	finish.store(true);
	forceDisconnectThread.join();

	GTEST_ASSERT_TRUE(queue->isEmpty());
	GTEST_ASSERT_TRUE(send_complete.load());
	GTEST_ASSERT_EQ(listener->getNumberOfAcknowledgedMessages(), num_messages);

	// TODO We may have to consider not using this as a check. Under load I've seen that a connection close request gets refused and the connection isn't actually broken.
	LOG_DEBUG("Number of forced disconnects: " << num_forced_reconnections);
	GTEST_ASSERT_LE(abs(controller.getNumReconnections() - num_forced_reconnections), 1) << "Not all threads within 1 reconnections. Expected reconnections between " << num_forced_reconnections -1 << " and " << num_forced_reconnections + 1
			<< " but got " << controller.getNumReconnections();
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
	rmq::ChannelConfig config {"testReceiveChannelAsync_exchange"
		, "testReceiveChannelAsync_queue"
		, "testReceiveChannelAsync_routing"};
	config.qos_prefetch_count = 0;
	auto rx_wrapper = controller.createReceiveChannel(config);
	auto tx_wrapper = controller.createTransmitChannel(config);

	auto tx_channel_listener = tx_wrapper.getListener();
	auto rx_channel_listener = rx_wrapper.getListener();
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
	const auto tx_queue = tx_wrapper.getQueue();
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
	GTEST_ASSERT_TRUE(rx_wrapper.getQueue() != nullptr);
	std::atomic<size_t> received_messages {0};
	std::atomic<bool> finish{false};
	std::jthread receive_thread([&rx_wrapper, &finish, &received_messages]()
	{
		while (!finish.load())
		{
			if (!rx_wrapper.getQueue()->isEmpty())
			{
				auto message = rx_wrapper.getQueue()->pop();
				++received_messages;
				rx_wrapper.acknowledge(message.getAck());
				if (received_messages%10000 == 0)
				{
					LOG_INFO("Received and acknowledged message " << received_messages << ", ack queue has " << rx_wrapper.getAckQueue()->size() << " messages");
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
	constexpr size_t num_messages = 1E5;
	constexpr size_t num_channels = 5;
	GTEST_LOG_(INFO) << "Test that we can receive " << num_messages << " messages successfully on " << num_channels << " channels in parallel";
	testMultipleTxRxChannelsAsync_(num_messages, num_channels);
}



void TestAmqp::testMultipleTxRxChannelsAsync_(const size_t num_messages, const size_t num_channels)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	controller.setMaxTransmitBatchSize(200); // TODO Probably need to set based on the number of channels = e.g. 2000 / num_channels
	controller.start();

	// Create all of the channels
	std::vector<TxClientWrapper> tx_wrappers;
	std::vector<RxClientWrapper> rx_wrappers;
	for (auto i=0; i<num_channels; i++)
	{
		rmq::ChannelConfig config {"testMultipleTxRxChannelsAsync_exchange_" + std::to_string(i)
			, "testMultipleTxRxChannelsAsync_exchange_queue_" + std::to_string(i)
			, "testMultipleTxRxChannelsAsync_routing_" + std::to_string(i)};
		tx_wrappers.emplace_back(controller.createTransmitChannel(config));
		rx_wrappers.emplace_back(controller.createReceiveChannel(config));
	}

	auto start = std::chrono::high_resolution_clock::now();
	while (!(controller.isConnectionReady()
		&& std::ranges::all_of(tx_wrappers, [](const auto &wrapper) { return wrapper.getListener()->isActive(); })
		&& std::ranges::all_of(rx_wrappers, [](const auto &wrapper) { return wrapper.getListener()->isActive(); }))
		   && std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(controller.isConnectionReady()) << "Connection is not ready";
	GTEST_ASSERT_TRUE(std::ranges::all_of(tx_wrappers, [](const auto &wrapper) { return wrapper.getListener()->isActive(); })) << "Tx Channels are not active";
	GTEST_ASSERT_TRUE(std::ranges::all_of(rx_wrappers, [](const auto &wrapper) { return wrapper.getListener()->isActive(); })) << "Rx Channels are not active";

	// Send some messages
	std::atomic send_complete(false);
	GTEST_ASSERT_TRUE(std::ranges::all_of(tx_wrappers, [](const auto &wrapper) { return wrapper.getQueue() != nullptr; })) << "EEk - one or more TX queues are nullptrs";
	std::jthread send_thread([&tx_wrappers, &send_complete, num_messages]()
	{
		for (size_t i = 0; i < num_messages; i++)
		{
			std::string message = "test message " + std::to_string(i);
			auto message_vec = std::make_shared<std::vector<char> >(message.begin(), message.end());
			for (auto &wrapper : tx_wrappers)
			{
				wrapper.getQueue()->push(message_vec);
			}
		}
		send_complete.store(true);
	});

	// Receive the messages
	std::atomic force_finish(false);
	std::atomic receive_complete(false);
	GTEST_ASSERT_TRUE(std::ranges::all_of(rx_wrappers, [](const auto &wrapper) { return wrapper.getQueue() != nullptr; })) << "EEk - one or more RX queues are nullptrs";
	std::jthread receive_thread([&rx_wrappers, &force_finish, &receive_complete, num_messages]()
	{
		while (!force_finish.load()
			&& !std::ranges::all_of(rx_wrappers, [num_messages](const auto &wrapper) { return wrapper.getListener()->getNumberOfAcknowledgedMessages() == num_messages; }))
		{
			bool received_message = false;
			for (auto &wrapper : rx_wrappers)
			{
				if (!wrapper.getQueue()->isEmpty())
				{
					auto message = wrapper.getQueue()->pop();
					wrapper.acknowledge(message.getAck());
					received_message = true;
				}
			}
			if (!received_message)
			{
				LOG_TRACE("Queues are empty");
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
		}

		receive_complete.store(true);
	});

	start = std::chrono::high_resolution_clock::now();
	auto timeout = std::max({getTransmitTimeout_(num_messages), getReceiveTimeout_(num_messages), std::chrono::seconds(300)});
	LOG_INFO("Waiting for all the messages to have been transmitted and received: " << timeout.count() << " seconds");
	while (!(receive_complete.load() && send_complete.load())
		&& std::chrono::high_resolution_clock::now() - start < timeout
	)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	force_finish.store(true);
	receive_thread.join();
	send_thread.join();
	for (auto &wrapper : rx_wrappers)
	{
		LOG_INFO("Received " << wrapper.getListener()->getNumberOfReceivedMessages() << " messages on " << wrapper.getChannelName());
	}

	GTEST_ASSERT_TRUE(std::ranges::all_of(rx_wrappers, [num_messages](const auto &wrapper) { return wrapper.getListener()->getNumberOfAcknowledgedMessages() == num_messages; }));
}


TEST_F(TestAmqp, testSingleTxMultipleRx_short)
{
	constexpr size_t num_messages = 10000;
	constexpr size_t num_rx_channels = 2;
	GTEST_LOG_(INFO) << "Test that we can receive " << num_messages << " messages successfully on " << num_rx_channels << " receive channels";
	testSingleTxMultipleRx_(num_messages, num_rx_channels);
}

void TestAmqp::testSingleTxMultipleRx_(const size_t num_messages, const size_t num_rx_channels)
{
	// Basic setup
	rmq::MyAmqpController controller("amqp://guest:guest@localhost/");
	controller.start();

	// Core configuration
	rmq::ChannelConfig config {"testSingleTxMultipleRx_exchange_"
			, ""
			, "testSingleTxMultipleRx_routing"};
	config.qos_prefetch_count = 200;

	// Need a single transmitter
	const auto tx_channel_listener = std::make_shared<SimpleChannelListener>();
	const std::string tx_channel_name = controller.createTransmitChannel(config, tx_channel_listener).getChannelName();

	std::set<RxClientWrapper> rx_clients;
	for (int i=0; i<num_rx_channels; i++)
	{
		auto rx_config = config;
		rx_config.queue_name = "testSingleTxMultipleRx_queue_" + std::to_string(i);
		rx_clients.emplace(RxClientWrapper(controller.createReceiveChannel(rx_config)));
	}

	// Ensure we're up and running
	auto start = std::chrono::high_resolution_clock::now();
	while (!(controller.isConnectionReady()
			 && tx_channel_listener->isActive()
			 && std::ranges::all_of(rx_clients,[](const auto& entry) { return entry.getListener()->isActive(); })
			 )
		   && std::chrono::high_resolution_clock::now() - start < std::chrono::seconds(2))
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	GTEST_ASSERT_TRUE(controller.isConnectionReady()) << "Connection is not ready on " << config.exchange_name;
	GTEST_ASSERT_TRUE(tx_channel_listener->isActive()) << "Channel is not active on " << tx_channel_name;
	GTEST_ASSERT_TRUE(std::ranges::all_of(rx_clients,[](const auto& entry) { return entry.getListener()->isActive(); })) << "Rx Channels are not all active";

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

	// Need num_rx_channels worth of receivers
	std::atomic<bool> finish{false};
	std::jthread receive_thread([&rx_clients, &finish]()
	{
		while (!finish.load())
		{
			bool all_empty = true;
			for (auto& entry : rx_clients)
			{
				if (!entry.getQueue()->isEmpty())
				{
					auto message = entry.getQueue()->pop();
					entry.acknowledge(message.getAck());
					all_empty = false;
				}
			}
			if (all_empty)
			{
				LOG_TRACE("Queue is empty");
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
		}
	});


	// Wait until all receivers have finished or timed out
	start = std::chrono::high_resolution_clock::now();
	auto timeout = std::max({getTransmitTimeout_(num_messages), getReceiveTimeout_(num_messages), std::chrono::seconds(50)});
	LOG_INFO("Waiting for all the messages to have been transmitted and received: " << timeout.count() << " seconds");
	while (!std::ranges::all_of(rx_clients, [num_messages](const auto& entry) { return entry.getListener()->getNumberOfReceivedMessages() == num_messages; })
	       && std::chrono::high_resolution_clock::now() - start < timeout)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	finish.store(true);
	send_thread.join();
	receive_thread.join();

	GTEST_ASSERT_TRUE(std::ranges::all_of(rx_clients, [num_messages](const auto& entry) { return entry.getListener()->getNumberOfReceivedMessages() == num_messages;} ));
}
