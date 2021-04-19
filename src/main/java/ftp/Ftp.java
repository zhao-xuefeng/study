package ftp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class Ftp {
    private static Log LOG= LogFactory.getLog(Ftp.class);
    private FSDataOutputStream fos =null;
    private FileOutputStream fo=null;
    private FileSystem fs=null;
    private  FTPClient ftp = new FTPClient();
//    递归遍历文件，这样的再liunx系统上不可以遍历会出现 ftpFiles 空指针异常，此时需要进入被动模式
    private void downloadFiles(String remoteFilePath,String dstFilePath,FTPClient ftp,FileSystem fs) throws IOException {
        if (!remoteFilePath.trim().isEmpty()) {
            FTPFile[]ftpFiles = ftp.listFiles(remoteFilePath);
            for (FTPFile file : ftpFiles) {
                if (file.isDirectory()) {
                    System.out.println(remoteFilePath+File.separator+file.getName()+"============");
                    downloadFiles(file.getName(), dstFilePath, ftp,fs);
                } else {
                    fos = fs.create(new Path(dstFilePath, file.getName()));
                    if (ftp.retrieveFile(remoteFilePath, fos)) {

                        LOG.info("upload file to hdfs successfully!");
                    } else {
                        LOG.warn("upload file to hdfs failed!");
                    }
                }
            }
        }else {
            LOG.warn("Please enter the correct file path!");
        }

    }
    //递归上传到ftp所在服务器上，这里并未创建相应的目录，sftp同理
    private void uploadFtpFromHdfs(Path hdfsPath,Boolean recursive,String ftpPath) throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(hdfsPath, recursive);
        while (iterator.hasNext()) {
            Path filePath = iterator.next().getPath();
            String fileName=ftpPath + File.separator+filePath.getName();
            try (FSDataInputStream fos=fs.open(filePath,16384);){//这样的写法会自动关闭
                ftp.storeFile(ftpPath,fos);
            }

        }
    }
    public static void main(String[] args) {
        FTPClient ftp = new FTPClient();
        InputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        boolean flag = true;
        try {
            ftp.connect("192.168.100.12");
            ftp.login("mysql", "mysql");
            ftp.setFileType(ftp.BINARY_FILE_TYPE);
            ftp.enterLocalPassiveMode();//被动模式
            ftp.setControlEncoding("UTF-8");
            int reply = ftp.getReplyCode();
            System.out.println(FTPReply.isPositiveCompletion(reply));
            System.out.println("-------------------------");
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
            }
            File file=new File("D:\\privateLearn\\data\\ftp.txt");
            FileOutputStream fo=new FileOutputStream(file);
            ftp.retrieveFile("/home/mysql/aa",fo);
            fo.close();
//            ftp.completePendingCommand();
            if (ftp.isConnected()){
                ftp.disconnect();
            }

//            ftp.remoteRetrieve("");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
