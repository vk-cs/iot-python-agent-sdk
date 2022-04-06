iot-python-agent-sdk
====================

SDK for building agents for VK IoT Platform in Python programming language.

## Usage

You can find sample programs in the `examples` directory.

## Installation and requirements

The library uses all the power of modern asynchronous Python, so the `python >= 3.7` version of the interpreter is required. 

You can install this package from source archive:

```bash
    pip install coiiot_client-1.0.0.tar.gz
```

## Development

### Tests

To run tests we should create a virtual environment:
```bash
    make tests-dep
```

Than we should run a mqtt broker for tests:
```bash
    make docker-run-mosquitto
```

All is ready to run test suite:
```bash
    make tests-run
```

### Distribution

Use this command to create a built distribution:

```bash
    python setup.py sdist --formats=zip
```

The distribution will be in the `dist` folder. 
