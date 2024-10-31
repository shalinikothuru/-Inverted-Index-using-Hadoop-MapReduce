Installation:
Install Java, Hadoop and IntelliJ IDEA IDE to run this project

Steps to run this project:
1. First download the data from the data folder.
2. Start hadoop services using start-all.cmd
3. Upload the data to /project folder to HDFS.
4. Open code/bda_project folder in IntelliJ IDE
5. Perform Maven clean and install
6. Use following command to run the MapReduce program
    cmd : ‘hadoop jar target/bda_project-1.0-SNAPSHOT.jar bda_project.runner /project/data /project/results’
7. You can view the results in hdfs.