from setuptools import setup, find_packages


setup(
    name="coiiot_client",
    version="1.0.0",
    packages=find_packages(include="coiiot_client.*"),

    install_requires=[
        'aiohttp==3.4.4',
        'hbmqtt==0.9.6',
        'simplejson==3.17.6',
    ],

    package_data={},

    # metadata to display on PyPI
    author="VK",
    author_email="",
    description="SDK for building agents for VK IoT Platform",
    keywords="",
    url="",   # project home page, if any
    project_urls={},
    classifiers=[]
)
