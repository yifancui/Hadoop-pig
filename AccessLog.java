package my;

import java.util.StringTokenizer;

/**
 * author: Han Jiang
 * time: Tue Sep 12, 2017
 */
public class AccessLog {
    private int AccessID;
    private int ByWho;
    private int WhatPage;
    private String TypeOfAccess;
    private int AccessTime;

    public AccessLog(int AccessID, int ByWho, int WhatPage, String TypeOfAccess, int AccessTime){
        super();
        this.AccessID = AccessID;
        this.ByWho = ByWho;
        this.WhatPage = WhatPage;
        this.TypeOfAccess = TypeOfAccess;
        this.AccessTime = AccessTime;
    }

    public AccessLog(String input){
        StringTokenizer itr = new StringTokenizer(input, ",");
        this.AccessID = Integer.parseInt(itr.nextToken());
        this.ByWho = Integer.parseInt(itr.nextToken());
        this.WhatPage = Integer.parseInt(itr.nextToken());
        this.TypeOfAccess = itr.nextToken();
        this.AccessTime = Integer.parseInt(itr.nextToken());
    }


    public int getAccessID() {
        return AccessID;
    }

    public void setAccessID(int accessID) {
        this.AccessID = accessID;
    }

    public int getByWho() {
        return ByWho;
    }

    public void setByWho(int byWho) {
        this.ByWho = byWho;
    }

    public int getWhatPage() {
        return WhatPage;
    }

    public void setWhatPage(int whatPage) {
        this.WhatPage = whatPage;
    }

    public String getTypeOfAccess() {
        return TypeOfAccess;
    }

    public void setTypeOfAccess(String typeOfAccess) {
        this.TypeOfAccess = typeOfAccess;
    }

    public int getAccessTime() {
        return AccessTime;
    }

    public void setAccessTime(int accessTime) {
        this.AccessTime = accessTime;
    }
}
