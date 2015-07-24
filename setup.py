try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

# http://bugs.python.org/issue8876
try:
    import os
    del os.link
except:
    pass

setup(
    name='redis_gadgets',
    author='Keep.com development team',
    author_email='opensource@keep.com',
    version='0.2.0',
    packages=find_packages(exclude=['tests*']),
    include_package_data=True,
    url='http://keep.com',
    license='MIT',
    description='Light-weight tools to implement high-level features in Redis',
    long_description=open('README.md').read(),
    zip_safe=False,
    install_requires=open('requirements.txt').readlines()
)
