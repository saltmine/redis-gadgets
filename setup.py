try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup


setup(
    name='redis_gadgets',
    author='Keep.com development team',
    author_email='opensource@keep.com',
    version='0.1.0',
    packages=find_packages(exclude=['tests*']),
    include_package_data=True,
    url='http://keep.com',
    license='MIT',
    description='Light-weight tools to implement high-level features in Redis',
    long_description=open('README.rst').read(),
    zip_safe=False,
    install_requires=open('requirements.txt').readlines()
)
