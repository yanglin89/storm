package com.run.stream.drpc;

/**
 * drpc
 * 用户的服务
 */
public interface UserService {

    public static final long versionID = 88888888;

    public void userAdd(String name,String age);

}
