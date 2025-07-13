#include <iostream>
#include <event2/event.h>
#include <amqpcpp.h>
#include <thread>
#include <amqpcpp/libevent.h>

#include "my_amqp_controller.hpp"

// TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
int main()
{
	constexpr int num_repeats = 1000000;

	for (int i = 0; i < num_repeats; i++)
	{
		constexpr int num_threads = 10;
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


	// done
	return 0;

}