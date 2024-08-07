# SDLE First Assignment

SDLE First Assignment of group T02G11.

## Members:

1. [Duarte Sardão](https://github.com/duarte-sardao)
2. [Henrique Sousa (Self)](https://github.com/henriquecscode)
3. [Mateus Silva](https://github.com/lessthelonely) 
4. [Melissa Silva](https://github.com/melisilva)

## How To Compile

### Pre-requisites
Before trying to compile and run this project, it's necessary to check if your machine has the ZeroMQ library for Python installed.

If not, please execute the following command in your terminal:

```bash
pip install pyzmq
```

### Running
To compile and run this project, the following commands should be executed:

```bash
python src/run_server.py # in order to run the server side of the program 
python src/run_client.py # in order to run the client side of the program
```

In order to test the program (and execute operations: put, get, subscribe, unsubscribe), it's necessary to follow the prompts given in the *run_client.py* file.
