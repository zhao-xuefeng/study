package genrateData;

import cn.binarywang.tools.generator.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.StringJoiner;

public class GenerateData {

    public static void main(String[] args) throws FileNotFoundException {

        //身份证号码
        ChineseIDCardNumberGenerator cidcng = (ChineseIDCardNumberGenerator) ChineseIDCardNumberGenerator.getInstance();
        //中文姓名
        ChineseNameGenerator cng = ChineseNameGenerator.getInstance();
        //英文姓名
        EnglishNameGenerator eng = EnglishNameGenerator.getInstance();
        //手机号
        ChineseMobileNumberGenerator cmng = ChineseMobileNumberGenerator.getInstance();
        //电子邮箱
        EmailAddressGenerator eag = (EmailAddressGenerator) EmailAddressGenerator.getInstance();

        //居住地址
        ChineseAddressGenerator cag = (ChineseAddressGenerator) ChineseAddressGenerator.getInstance();

        PrintWriter pw = new PrintWriter("D:\\data_1000w.csv");
        for (int i = 0; i < 10000000; i++) {
            StringJoiner sj = new StringJoiner(",");
            sj.add(cidcng.generate());
            sj.add(cng.generate());
            sj.add(eng.generate());
            sj.add(cmng.generate());
            sj.add(eag.generate());
            sj.add(cag.generate());
            pw.println(sj.toString());
        }
        pw.close();

    }

}