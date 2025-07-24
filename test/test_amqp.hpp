#pragma once
#include <cstdlib>
#include <gtest/gtest.h>
#include <thread>

#include "channel_config.hpp"
#include "my_amqp_controller.hpp"

class TestAmqp : public ::testing::Test
{
public:
	TestAmqp() {}
	virtual ~TestAmqp() {}

	void SetUp() override
	{
		clearQueues();
		clearExchanges();
	}

	// Clear-up previous queue data - assume that there is nothing else running on this test machine
	static void clearQueues()
	{
		const auto rc = system("rabbitmqadmin -f tsv -q list queues name | while read queue; do rabbitmqadmin -q delete queue name=${queue}; done");
		ASSERT_EQ(rc, EXIT_SUCCESS);
	}

	static void clearExchanges()
	{
		const auto rc = system(	"rabbitmqadmin -f tsv -q list exchanges name | grep test | while read exchange; do rabbitmqadmin -q delete exchange name=${exchange}; done");
		ASSERT_EQ(rc, EXIT_SUCCESS);
	}


private:

	class TxRxThreadEntry
	{
	public:
		explicit TxRxThreadEntry(const rmq::ChannelConfig& config) : config_(config), test_thread_(nullptr), finish_(false) {}
		virtual ~TxRxThreadEntry() = default;

		rmq::ChannelConfig getChannelConfig()
		{
			return config_;
		}

		void setTestThread(const std::shared_ptr<std::jthread>& test_thread)
		{
			test_thread_ = test_thread;
		}

		std::shared_ptr<std::jthread> getTestThread()
		{
			return test_thread_;
		}

		std::atomic<bool> &getFinish()
		{
			return finish_;
		}
	private:
		rmq::ChannelConfig config_;
		std::shared_ptr<std::jthread> test_thread_;
		std::atomic<bool> finish_;
	};

	// Force connections to close
	static std::jthread forceCloseConnections(std::atomic<bool>& finish, const std::chrono::milliseconds& interval, int& num_forced_reconnections);
	static int forceCloseConnections_();

	// Verification of start stop behaviour on the core connection and handler (excludes channels)
	static bool testStartStopRealNoChannel_(int num_repeats, int num_threads);
	static bool testForceReconnectNoChannel_(int num_repeats, int num_threads);
	FRIEND_TEST(TestAmqp, testStartStopRealNoChannel_short);
	FRIEND_TEST(TestAmqp, testStartStopRealNoChannel_long);
	FRIEND_TEST(TestAmqp, testReconnectionNoChannel_short);
	FRIEND_TEST(TestAmqp, testReconnectionNoChannel_long);

	// Verification of purely transmit items
	FRIEND_TEST(TestAmqp, testTransmitChannel_short);
	FRIEND_TEST(TestAmqp, testTransmitChannel_long);
	FRIEND_TEST(TestAmqp, testTransmitMultipleChannels_short);
	FRIEND_TEST(TestAmqp, testTransmitMultipleChannels_long);
	static void testTransmitChannelWithManager_(size_t num_messages, int num_channels = 1);
	static std::jthread send_data(std::vector<rmq::TxClientWrapper> &wrappers, std::atomic<bool>& send_complete, int num_messages);
	static std::chrono::seconds getTransmitTimeout_(const size_t num_messages);


	FRIEND_TEST(TestAmqp, testReconnectionTxChannel_short);
	FRIEND_TEST(TestAmqp, testReconnectionTxChannel_long);
	static void testTransmitChannelWithReconnect_(const size_t num_messages);

	// Verification of receive items
	FRIEND_TEST(TestAmqp, testReceiveChannel_short);
	FRIEND_TEST(TestAmqp, testReceiveChannel_long);
	static void testReceiveChannelAsync_(const size_t num_messages);

	FRIEND_TEST(TestAmqp, testTxRxMultipleSeparateChannels_short);
	FRIEND_TEST(TestAmqp, testTxRxMultipleSeparateChannels_long);
	static void testMultipleTxRxChannelsAsync_(size_t num_messages, size_t num_channels);
	static void testSingleTxRxChannelsAsync_(rmq::MyAmqpController &controller, const std::shared_ptr<TxRxThreadEntry> &entry, size_t num_messages);
	static std::chrono::seconds getReceiveTimeout_(const size_t num_messages);

	FRIEND_TEST(TestAmqp, testSingleTxMultipleRx_short);
	static void testSingleTxMultipleRx_(size_t num_messages, size_t num_rx_channels);

	// Example tests looking at specific core functionality and stability
	static bool testStartStopExampleWithSingleChannel_(int num_repeats, int num_threads);
	static bool testStartStopExampleWithNoChannel_(int num_repeats, int num_threads);
	FRIEND_TEST(TestAmqp, testStartStopExampleWithSingleChannel_short);
	FRIEND_TEST(TestAmqp, testStartStopExampleWithNoChannel_short);
};
