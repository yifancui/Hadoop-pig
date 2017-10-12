package my;

import java.util.StringTokenizer;

/**
 * author: Han Jiang
 * time: Tue Sep 12, 2017
 */
public class Friends {
    private int FriendRel;
    private int PersonID;
    private int MyFriend;
    private int DateofFriendship;
    private String Desc;

    public Friends(int FriendRel, int PersonID, int MyFriend, int DateofFriendship, String Desc){
        super();
        this.FriendRel = FriendRel;
        this.PersonID = PersonID;
        this.MyFriend = MyFriend;
        this.DateofFriendship = DateofFriendship;
        this.Desc = Desc;
    }

    public Friends(String input){
        StringTokenizer itr = new StringTokenizer(input, ",");
        this.FriendRel = Integer.parseInt(itr.nextToken());
        this.PersonID = Integer.parseInt(itr.nextToken());
        this.MyFriend = Integer.parseInt(itr.nextToken());
        this.DateofFriendship = Integer.parseInt(itr.nextToken());
        this.Desc = itr.nextToken();
    }


    public int getFriendRel() {
        return FriendRel;
    }

    public void setFriendRel(int friendRel) {
        this.FriendRel = friendRel;
    }

    public int getPersonID() {
        return PersonID;
    }

    public void setPersonID(int personID) {
        this.PersonID = personID;
    }

    public int getMyFriend() {
        return MyFriend;
    }

    public void setMyFriend(int myFriend) {
        this.MyFriend = myFriend;
    }

    public int getDateofFriendship() {
        return DateofFriendship;
    }

    public void setDateofFriendship(int dateofFriendship) {
        this.DateofFriendship = dateofFriendship;
    }

    public String getDesc() {
        return Desc;
    }

    public void setDesc(String desc) {
        this.Desc = desc;
    }
}
