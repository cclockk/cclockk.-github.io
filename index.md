
# 调用MapReduce对文件中各个单词出现的次数进行统计



## **一、数据上传**
数据来源于英文原版电子书网站https://www.gutenberg.org/  
![图片](https://user-images.githubusercontent.com/96853550/147720287-381cc0a1-1d72-403c-8d24-279bc004da8d.png)  
下载Plain Text UTF-8选项，全书共80985字

使用如下命令把本地文件系统的“/home/hadoop/Frankenstein.txt ”上传到HDFS中的当前用户目录的input目录下，也就是上传到HDFS的“/user/hadoop/input/”目录下：   
`./bin/hdfs dfs -put /home/hadoop/Frankenstein.txt  input`

![图片](https://user-images.githubusercontent.com/96853550/147720918-1095bcb5-00c1-4264-ad0b-09ea34f023e8.png)  

使用ls命令查看一下文件是否成功上传到HDFS中，具体如下： 
 `./bin/hdfs dfs -ls input` 


![图片](https://user-images.githubusercontent.com/96853550/147721090-827a6590-e42c-4a5c-9ea3-badab7b3bc72.png)



## **二、数据处理**

### 1.启动Eclipse  

```
$cd /usr/lib/eclipse
./eclipse 
```
![图片](https://user-images.githubusercontent.com/96853550/147721361-d94ffc50-4565-47c1-9614-a5e3ef037d33.png)  



### 2.在Eclipse中创建项目

启动以后会弹出如下图所示界面，提示设置工作空间（workspace）  
这里直接采用默认的设置“/home/hadoop/workspace”，点击“OK”按钮。
![图片](https://user-images.githubusercontent.com/96853550/147721404-7f797ded-2da1-47c2-ac24-2229c1dddb0c.png)

选择“File–>New–>Java Project”菜单，开始创建一个Java工程。  
在“Project name”后面输入工程名称“WordCount”，选中“Use default location”，让这个Java工程的所有文件都保存到“/home/hadoop/workspace/WordCount”目录下。在“JRE”这个选项卡中，可以选择当前的Linux系统中已经安装好的JDK，比如jdk1.8.0_162。然后，点击界面底部的“Next>”按钮，进入下一步的设置。
![图片](https://user-images.githubusercontent.com/96853550/147721490-706b2763-b9cd-4111-b514-6e60ab168ed2.png)  



### 3.为项目添加需要用到的JAR包

点击界面中的“Libraries”选项卡，然后，点击界面右侧的“Add External JARs…”按钮，在这个界面中加载该Java工程所需要用到的JAR包。  
![图片](https://user-images.githubusercontent.com/96853550/147721532-77c08e74-9fbb-4762-abd2-244cc7c59d13.png)  
（1）“/usr/local/hadoop/share/hadoop/common”目录下的hadoop-common-3.1.3.jar和haoop-nfs-3.1.3.jar；  
（2）“/usr/local/hadoop/share/hadoop/common/lib”目录下的所有JAR包；  
（3）“/usr/local/hadoop/share/hadoop/mapreduce”目录下的所有JAR包，但是，不包括jdiff、lib、lib-examples和sources目录，具体如下图所示。  
全部添加完毕以后，就可以点击界面右下角的“Finish”按钮，完成Java工程WordCount的创建。  
![图片](https://user-images.githubusercontent.com/96853550/147721588-f3b50491-160c-4b58-968e-89ff7bb6a595.png)  
![图片](https://user-images.githubusercontent.com/96853550/147721590-8b3e58e9-b465-4c47-9a6c-8ff03cdaf472.png)  
![图片](https://user-images.githubusercontent.com/96853550/147721592-d06c416d-f978-4a71-8f2c-71208b18f4b6.png)  



### 4.编写Java应用程序

在Eclipse工作界面左侧的“Package Explorer”面板中，找到工程“WordCount”，然后在该工程名称上点击鼠标右键，在弹出的菜单中选择“New–>Class”菜单。
在“Name”后面输入新建的Java类文件的名称“WordCount”，其他默认。  
![图片](https://user-images.githubusercontent.com/96853550/147721664-1f05f7be-7ac8-4701-a9d0-d740432a4d63.png)  

清空该文件里面的代码，然后在该文件中输入完整的词频统计程序代码，具体如下：  
```
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class WordCount {
    public WordCount() {
    }
     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if(otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 
        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public TokenizerMapper() {
        }
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()); 
            while(itr.hasMoreTokens()) {
                this.word.set(itr.nextToken());
                context.write(this.word, one);
            }
        }
    }
public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public IntSumReducer() {
        }
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val;
            for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                val = (IntWritable)i$.next();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }
    }
}
```



### 5.编译打包程序

点击Eclipse工作界面上部的运行程序的快捷按钮，在弹出的菜单中选择“Run as”，继续在弹出来的菜单中选择“Java Application”  
![图片](https://user-images.githubusercontent.com/96853550/147721729-7cea33f1-0dd6-4b53-b046-14074e72c4cf.png)

程序运行结束后，会在底部的“Console”面板中显示运行结果信息  
![图片](https://user-images.githubusercontent.com/96853550/147721737-e5c8c514-1548-42ad-ad13-ecb90af6cd98.png)

把词频统计程序放在“/usr/local/hadoop/myapp”目录下。如果该目录不存在，可以使用如下命令创建  
```
$cd /usr/local/hadoop
mkdir myapp
```

Eclipse工作界面左侧的“Package Explorer”面板→在工程名称“WordCount”上点击鼠标右键→在弹出的菜单中选择“Export”。    
![图片](https://user-images.githubusercontent.com/96853550/147721900-b439fd83-2e4f-42c8-a474-6d0f0f8fd1a0.png)    
![图片](https://user-images.githubusercontent.com/96853550/147721906-7f1d8305-7899-4462-8bb9-c5aace74afa4.png)     
在该界面中，“Launch configuration”用于设置生成的JAR包被部署启动时运行的主类，需要在下拉列表中选择刚才配置的类“WordCount-WordCount”。在“Export destination”中需要设置JAR包要输出保存到哪个目录，比如，这里设置为“/usr/local/hadoop/myapp/WordCount.jar”。在“Library handling”下面选择“Extract required libraries into generated JAR”。然后，点击“Finish”按钮，会出现如下图所示界面。  
![图片](https://user-images.githubusercontent.com/96853550/147721955-396628b5-e039-4f0d-babf-51f6903297a0.png)    
可以忽略该界面的信息，直接点击界面右下角的“OK”按钮，启动打包过程。打包过程结束后，会出现一个警告信息界面，如下图所示。  
![图片](https://user-images.githubusercontent.com/96853550/147721974-6b5a1a40-61b0-4386-b209-ad8f81884b9f.png)  
可以忽略该界面的信息，直接点击界面右下角的“OK”按钮。  

打包完后到Linux系统中查看一下生成的WordCount.jar文件，可以在Linux的终端中执行如下命令：  
```
cd /usr/local/hadoop/myapp
ls
```
![图片](https://user-images.githubusercontent.com/96853550/147722000-cc0e3296-1a7a-421a-9f3f-8219535179df.png)  



### 6.运行程序 

在运行程序之前，需要启动Hadoop  
```
cd /usr/local/hadoop
./sbin/start-dfs.sh
```
![图片](https://user-images.githubusercontent.com/96853550/147722117-1d715687-59cb-4c31-8bcc-8ad47b44ad4e.png)  

如果HDFS中已经存在目录“/user/hadoop/output”，则使用如下命令删除该目录：  
```
cd /usr/local/hadoop
./bin/hdfs dfs -rm -r /user/hadoop/output
```

使用hadoop jar命令运行程序，命令如下：  
```
$cd /usr/local/hadoop
./bin/hadoop jar ./myapp/WordCount.jar input output
```
当运行顺利结束时，屏幕上会显示类似如下的信息：  
![图片](https://user-images.githubusercontent.com/96853550/147722291-ba91b567-9dfa-4c7a-93a4-a808432c0435.png)  



## **三、数据下载**

词频统计结果已经被写入了HDFS的“/user/hadoop/output”目录中，可以执行如下命令查看词频统计结果：  
```
$cd /usr/local/hadoop
./bin/hdfs dfs -cat output/*
```
![图片](https://user-images.githubusercontent.com/96853550/147722357-1de247f2-a311-43a8-831b-90aed1f052c5.png)  
如果要再次运行WordCount.jar，需要首先删除HDFS中的output目录，否则会报错。  

将统计结果下载本地。  
`./bin/hdfs dfs -get output/part-r-00000  /home/hadoop/下载`
![图片](https://user-images.githubusercontent.com/96853550/147722393-d8c9c11b-a8dd-44d5-9feb-73f8df5d45c2.png)  
![图片](https://user-images.githubusercontent.com/96853550/147722397-711c6b7c-be96-486d-8287-86deb900c605.png)      




参考文献  
[1]林子雨.大数据技术原理与应用.第3版.北京:人民邮电出版社，2021.
