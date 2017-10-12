package my;

import java.util.StringTokenizer;

/**
 * By Han Jiang on 9/14/2017
 */
public class MyPage {
    private int ID;
    private String Name;
    private String Nationality;
    private int CountryCode;
    private String Hobby;

    public MyPage(int ID, String Name, String Nationality, int CountryCode, String Hobby){
        super();
        this.ID = ID;
        this.Name = Name;
        this.Nationality = Nationality;
        this.CountryCode = CountryCode;
        this.Hobby = Hobby;
    }

    public MyPage(String input){
        StringTokenizer itr = new StringTokenizer(input, ",");
        this.ID = Integer.parseInt(itr.nextToken());
        this.Name = itr.nextToken();
        this.Nationality = itr.nextToken();
        this.CountryCode = Integer.parseInt(itr.nextToken());
        this.Hobby = itr.nextToken();
    }

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        this.Name = name;
    }

    public String getNationality() {
        return Nationality;
    }

    public void setNationality(String nationality) {
        this.Nationality = nationality;
    }

    public int getCountryCode() {
        return CountryCode;
    }

    public void setCountryCode(int countryCode) {
        this.CountryCode = countryCode;
    }

    public String getHobby() {
        return Hobby;
    }

    public void setHobby(String hobby) {
        this.Hobby = hobby;
    }
}
