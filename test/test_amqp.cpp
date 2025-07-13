#include "test_amqp.hpp"
#include "my_amqp_controller_example.hpp"
#include "my_amqp_controller_no_channel.hpp"

TEST_F(TestAmqp, testStartStopExampleWithSingleChannel_short)
{
	GTEST_LOG_(INFO) << "Start and stop one channel a thousand times.";
	GTEST_ASSERT_TRUE(testStartStopExampleWithSingleChannel_(1, 1));
	GTEST_ASSERT_TRUE(testStartStopExampleWithSingleChannel_(1000, 10));
}

bool TestAmqp::testStartStopExampleWithSingleChannel_(int num_repeats, int num_threads)
{
	for (int i = 0; i < num_repeats; i++)
	{
		std::vector<std::thread> myThreads(num_threads);
		for (auto &thread : myThreads)
		{
			thread = std::thread([]() {
				MyAmqpController controller;
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

