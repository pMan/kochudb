package com.kochudb.k;

public final class K {
	
	// tree
	public static final int DEFAULT_MEMTABLE_MAX_SIZE = 2;
	public static final int VALUE_MAX_SIZE = (1 << 20) * 4; // value of a key to set
	
	// file
	public static final String INDEX_FILE_EXT = ".idx";
	public static final String DATA_FILE_EXT = ".dat";
	public static final String L0_INDEX_FILE_PATTERN = "^[0-9]+\\.[0-9]+\\.idx$";
	public static final String L0_DATA_FILE_PATTERN = "^[0-9]+\\.[0-9]+\\.dat$";
	
	public static final String INDEX_FILE_PATTERN = "^l[0-9]+_[0-9]+\\.[0-9]+\\.idx$";
	public static final String DATA_FILE_PATTERN = "^l[0-9]+_[0-9]+\\.[0-9]+\\.dat$";
	
	// compaction
	public static final int LEVEL_ZERO_NUM_FILES = 5;
	public static final int LEVEL_ZERO_FILE_MAX_SIZE_KB = 4;
	public static final int LEVEL_MAX_SIZE_MULTIPLIER = 2;
	public static final int LEVEL_MAX_FILES_MULTIPLIER = 2;
	
	public static final int NUM_LEVELS = 5;
	
	// error codes
	public static final int ERR_INVALID_CLI_ARGS = 1;
	public static final int ERR_NO_DATA_DIR = 2;
	
	// CLI
	public static final String USAGE_HELP = ""
			+ "Invalid number of arguments\r\n\r\n"
			+ "Correct usage:\r\n\t"
			+ "java -jar kochudb.jar\r\n";
	
	// https://patorjk.com/software/taag/#p=display&f=Big&t=KochuDB
	public static final String WELCOME_BANNER = "\r\n"
			+ ":::::::::::::::::::::::::::::::::::::::::::::::::::\r\n"
			+ "::                                               ::\r\n"
			+ "::   Welcome to      _           _____  ____     ::\r\n"
			+ "::   | |/ /         | |         |  __ \\|  _ \\    ::\r\n"
			+ "::   | ' / ___   ___| |__  _   _| |  | | |_) |   ::\r\n"
			+ "::   |  < / _ \\ / __| '_ \\| | | | |  | |  _ <    ::\r\n"
			+ "::   | . \\ (_) | (__| | | | |_| | |__| | |_) |   ::\r\n"
			+ "::   |_|\\_\\___/ \\___|_| |_|\\__,_|_____/|____/    ::\r\n"
			+ "::                                 Version 0.1   ::\r\n"
			+ ":::::::::::::::::::::::::::::::::::::::::::::::::::\r\n";
}
