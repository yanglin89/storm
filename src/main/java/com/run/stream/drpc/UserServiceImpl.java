package com.run.stream.drpc;

/**
 * 用户服务接口实现类
 */
public class UserServiceImpl implements UserService{


    @Override
    public void userAdd(String name, String age) {
        System.out.println("Server add User : " + name + " success.");
    }
}
