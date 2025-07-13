#pragma once
#include <cstdlib>
#include <gtest/gtest.h>

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

	// Workup of real classes to be used in production
	static bool testStartStopRealNoChannel_(int num_repeats, int num_threads);
	static bool testStartStopRealSingleChannel_(int num_repeats, int num_threads);
	FRIEND_TEST(TestAmqp, testStartStopRealNoChannel_short);
	FRIEND_TEST(TestAmqp, testStartStopRealSingleChannel_short);

	// Example tests looking at specific core functionality and stability
	static bool testStartStopExampleWithSingleChannel_(int num_repeats, int num_threads);
	static bool testStartStopExampleWithNoChannel_(int num_repeats, int num_threads);
	FRIEND_TEST(TestAmqp, testStartStopExampleWithSingleChannel_short);
	FRIEND_TEST(TestAmqp, testStartStopExampleWithNoChannel_short);
};
