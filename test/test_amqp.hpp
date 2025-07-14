#pragma once
#include <cstdlib>
#include <gtest/gtest.h>
#include <thread>

class TestAmqp : public ::testing::Test
{
public:
	TestAmqp() {}
	virtual ~TestAmqp() {}

	void SetUp() override
	{
		clearQueues();
	}

	// Clear-up previous queue data - assume that there is nothing else running on this test machine
	static void clearQueues()
	{
		auto rc = system("rabbitmqadmin -f tsv -q list queues name | while read queue; do rabbitmqadmin -q delete queue name=${queue}; done");
		ASSERT_EQ(rc, EXIT_SUCCESS);
	}
private:

	// Force connections to close
	std::thread forceCloseConnections(std::atomic<bool>& finish, std::chrono::milliseconds& interval);

	// Verification of start stop behaviour on the core connection and handler (excludes channels)
	static bool testStartStopRealNoChannel_(int num_repeats, int num_threads);
	bool testForceReconnectNoChannel_(int num_repeats, int num_threads);
	FRIEND_TEST(TestAmqp, testStartStopRealNoChannel_short);
	FRIEND_TEST(TestAmqp, testStartStopRealNoChannel_long);
	FRIEND_TEST(TestAmqp, testReconnectionNoChannel_short);
	FRIEND_TEST(TestAmqp, testReconnectionNoChannel_long);

	// Verification of purely transmit items
	FRIEND_TEST(TestAmqp, testTransmitChannel_short_);


	// Example tests looking at specific core functionality and stability
	static bool testStartStopExampleWithSingleChannel_(int num_repeats, int num_threads);
	static bool testStartStopExampleWithNoChannel_(int num_repeats, int num_threads);
	FRIEND_TEST(TestAmqp, testStartStopExampleWithSingleChannel_short);
	FRIEND_TEST(TestAmqp, testStartStopExampleWithNoChannel_short);
};
