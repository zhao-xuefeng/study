package ftp;

import com.jcraft.jsch.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Vector;

public class Sftp {
//    递归从sftp所在服务器下载到hdfs
    public void downloadFiles(String srcFilePath, String hdfsPath, ChannelSftp sftp, FileSystem fs) throws SftpException, IOException {
        Vector ls = sftp.ls(srcFilePath);
        for (Object object : ls) {
            ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) object;
            String fileName = entry.getFilename();
//            不包含。目录
            if (".".equals(fileName) || "..".equals(fileName)) {
                continue;
            }
            if (entry.getAttrs().isDir()) {
                downloadFiles(srcFilePath + fileName, hdfsPath, sftp, fs);
            } else {
                try (FSDataOutputStream fos = fs.create(new Path(hdfsPath, fileName))) {
                    String[] files = srcFilePath.split(File.separator);
                    String file = files[files.length - 1];
                    if (file.equals(fileName)) {
                        sftp.get(srcFilePath, fos);
                    } else {
                        sftp.get(srcFilePath + File.separator + fileName, fos);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) throws JSchException, SftpException {
        JSch jSch = new JSch();
        String host = "10.130.7.154";
        int port = 22;
        Session session = null;

        session = jSch.getSession("mysftp", host, port);
        session.setPassword("bonc@321!@#");
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");

        session.setConfig(config);
        session.connect();
//        LOG.info("Session is connected");

        Channel channel = session.openChannel("sftp");
        channel.connect();
//        LOG.info("channel is connected");

        ChannelSftp sftp = (ChannelSftp) channel;
//        log.info(String.format("sftp server host:[%s] port:[%s] is connect successfull", host, port));
        File file = new File("C:\\bonc\\works\\a.txt");
        sftp.get("/upload/aaaa.csv", "D:\\ceshi\\sftp");
        if (sftp != null) {
            if (sftp.isConnected()) {
                sftp.disconnect();
//                LOG.info("sftp is closed already");
            }
        }
        if (session != null) {
            if (session.isConnected()) {
                session.disconnect();
//                LOG.info("sshSession is closed already");
            }
        }
    }
}
