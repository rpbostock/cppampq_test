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
	static bool testStartStopWithSingleChannel_(int num_repeats, int num_threads);
	static bool testStartStopWithNoChannel_(int num_repeats, int num_threads);
	FRIEND_TEST(TestAmqp, testStartStopWithSingleChannel_short);
	FRIEND_TEST(TestAmqp, testStartStopWithNoChannel_short);
};
