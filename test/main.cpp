#include <logging.hpp>
#include <gtest/gtest.h>

int main(int argc, char *argv[])
{
	// Reducing log output for normal runs
	Logging::init(true, true, int(boost::log::trivial::info), int(boost::log::trivial::info));

	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
