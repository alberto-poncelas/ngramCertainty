# ngramCertainty


A tool for computing the alignment entropy of n-grams.

## Building

In order to build the project, run the command:
```
sbt package
```
It will generate a jar file in the path `/target/scala-2.11/ngramcertainty_2.11-1.0.jar`

## Run an example

We provide some sample data in `data` folder for running an example. First edit the path to spark and the path of the project:
```
SPARK_PATH=path/to/spark
NGRCERT_PATH=path/to/ngramCertainty
```
Then execute the program.
```
pathFrom=$NGRCERT_PATH/data/parallel_data.src
pathTo=$NGRCERT_PATH/data/parallel_data.trg
pathTestSet=$NGRCERT_PATH/data/test_set
outputPath=$NGRCERT_PATH/data

$SPARK_PATH/bin/spark-submit $NGRCERT_PATH/target/scala-2.10/ngramcertainty_2.10-2.1.jar $pathFrom $pathTo $pathTestSet $outputPath
```

## Citation

If you use the program, consider citing:
```
@article{poncelas2017applying,
  title={Applying N-gram Alignment Entropy to Improve Feature Decay Algorithms},
  author={Poncelas, Alberto and Maillette de Buy Wenniger, Gideon and Way, Andy},
  journal={The Prague Bulletin of Mathematical Linguistics},
  volume={108},
  number={1},
  pages={245--256},
  year={2017},
}
```

