#include <logging.hpp>
#include <gtest/gtest.h>
#include <chrono>
#include <howardhinnant/date.h>

int main(int argc, char *argv[])
{
	// Reducing log output for normal runs
	auto now = std::chrono::system_clock::now();
	const auto datetime = date::format("{:%Y%m%d_%H%M%S}", now);
	std::string log_file_name = "test_cppamqp_" + datetime + ".log";
	Logging::init(true, true, int(boost::log::trivial::info), int(boost::log::trivial::debug), log_file_name);

	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
