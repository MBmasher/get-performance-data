### What does this do?

This gets the latest dataset from [data.ppy.sh](https://data.ppy.sh), unzips it and converts the many sql files into csv files.


### Initialising

Clone the repo:

```shell
git clone https://github.com/MBmasher/get-performance-data
cd get-performance-data
```

You can update the repo by running the following command:

```shell
git pull
```

Run the following command to execute the Makefile:

```shell
make
```

### Running

Simply run this command from the repo directory to run the script:

```shell
./get_performance_data.py
```

Add the parameter `-h` to get help on how to use the parameters.
