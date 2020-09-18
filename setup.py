import setuptools

setuptools.setup(
    name="pysql-bdjohnson529", # Replace with your own username
    version="0.0.1",
    author="Ben Johnson",
    description="Data analysis in Python and SQL",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires =	['pandas>=1.0.1',
						'numpy>=1.18.1',
						'pyodbc>=4.0.0',
                	]
)