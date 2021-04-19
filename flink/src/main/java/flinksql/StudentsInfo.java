package flinksql;

public class StudentsInfo {
    private String name;
    private String sex;
    private String cource;
    private Float socre;

    public String getName() {
        return name;
    }

    public Float getSocre() {
        return socre;
    }

    public void setSocre(Float socre) {
        this.socre = socre;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getCource() {
        return cource;
    }

    public void setCource(String cource) {
        this.cource = cource;
    }



}
