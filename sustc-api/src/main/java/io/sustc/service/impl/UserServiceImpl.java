package io.sustc.service.impl;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.nio.LongBuffer;
import java.util.ArrayList;
import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static io.sustc.service.impl.Utils.*;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;

    @Override
    public long register(RegisterUserReq req) {
        if (!isValidInput(req)) {
            return -1;
        }
        try(Connection connection = dataSource.getConnection();){
            if (isUserExist(connection,req)) {
                return -1;
            }
            return insertUser(connection,req);
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private boolean isValidInput(RegisterUserReq req) {
        if (req.getPassword() == null || req.getPassword().isEmpty() ||
                req.getName() == null || req.getName().isEmpty() ||
                req.getSex() == null) {
            return false;
        }
        if(req.getBirthday()==null)return true;
        if (req.getBirthday() != null && !req.getBirthday().isEmpty() &&
                !isValidDate(req.getBirthday())) {
            return false;
        }

        return true;
    }


    private boolean isUserExist(Connection connection,RegisterUserReq req) {
        boolean e1=req.getQq() != null && !req.getQq().isEmpty();
        boolean e2=req.getWechat() != null && !req.getWechat().isEmpty();
        if(!e1&&!e2)return false;
        String sql="";
        if(e1&&!e2)sql="SELECT 1 FROM Users WHERE qq = ?;";
        else if(!e1&&e2)sql="SELECT 1 FROM Users WHERE wechat = ?;";
        else sql = "SELECT 1 FROM Users WHERE qq = ? OR wechat = ?;";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            if(e1&&!e2)ps.setString(1,req.getQq());
            else if(!e1&&e2)ps.setString(1,req.getWechat());
            else {ps.setString(1,req.getQq());ps.setString(2,req.getWechat());}
            try(ResultSet rs = ps.executeQuery()){
                if(rs.next()){
                    return true;
                }else return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }

    private long insertUser(Connection connection,RegisterUserReq req) {
        try (PreparedStatement ps = connection.prepareStatement(
                     "INSERT INTO Users (name, password, sex, birthday, qq, wechat,sign,identity,coin,level) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?)",
                     PreparedStatement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, req.getName());
            ps.setString(2, req.getPassword());
            ps.setString(3, String.valueOf(req.getSex()));
            if(req.getBirthday()!=null){
                ps.setString(4, req.getBirthday());
            }else ps.setString(4,null);
            if(req.getQq()!=null){
                ps.setString(5, req.getQq());
            }else ps.setString(5, null);
            if(req.getWechat()!=null){
                ps.setString(6, req.getWechat());
            }else ps.setString(6, null);
            if(req.getSign()!=null){
                ps.setString(7, req.getSign());
            }else ps.setString(7, null);
            ps.setString(8,"USER");
            ps.setInt(9,0);
            ps.setInt(10,0);
            int affectedRows = ps.executeUpdate();
            if (affectedRows > 0) {
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }
    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        if (auth == null) {
            return false;
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement("SELECT identity,mid FROM Users WHERE mid = ? ;");
             PreparedStatement ps2 = connection.prepareStatement("DELETE FROM Users WHERE mid = ? ;\n" +
                     "DELETE FROM FollRela WHERE followerMid = ? ;\n" +
                     "DELETE FROM FollRela WHERE followeeMid = ? ;\n" +
                     "WITH DeletedRecords AS (\n" +
                     "    DELETE FROM ViewRela\n" +
                     "        WHERE viewerMid = ?\n" +
                     "        RETURNING bv\n" +
                     ")\n" +
                     "UPDATE Videos\n" +
                     "SET viewCount = viewCount - 1\n" +
                     "WHERE bv IN (SELECT bv FROM DeletedRecords);\n" +
                     "\n" +
                     "WITH DeletedLikeRecords AS (\n" +
                     "    DELETE FROM InteractionRela\n" +
                     "        WHERE mid = ? AND behavior = ?\n" +
                     "        RETURNING bv\n" +
                     ")\n" +
                     "UPDATE Videos\n" +
                     "SET likeCount = likeCount - 1\n" +
                     "WHERE bv IN (SELECT bv FROM DeletedLikeRecords);\n" +
                     "\n" +
                     "WITH DeletedCoinRecords AS (\n" +
                     "    DELETE FROM InteractionRela\n" +
                     "        WHERE mid = ? AND behavior = ?\n" +
                     "        RETURNING bv\n" +
                     ")\n" +
                     "UPDATE Videos\n" +
                     "SET coinCount = coinCount - 1\n" +
                     "WHERE bv IN (SELECT bv FROM DeletedCoinRecords);\n" +
                     "\n" +
                     "WITH DeletedCollectRecords AS (\n" +
                     "    DELETE FROM InteractionRela\n" +
                     "        WHERE mid = ? AND behavior = ?\n" +
                     "        RETURNING bv\n" +
                     ")\n" +
                     "UPDATE Videos\n" +
                     "SET collectCount = collectCount - 1\n" +
                     "WHERE bv IN (SELECT bv FROM DeletedCollectRecords);")
        ) {UserRecord rs2=isValidAuth(connection,auth);
            if(rs2==null)return false;
            ps.setLong(1, mid);
            try(ResultSet rs = ps.executeQuery();) {
                if (!rs.next()) return false;
                //user删别人
                if (String.valueOf(rs2.getIdentity()).equals("USER") && rs.getLong("mid") != (rs2.getMid()))
                    return false;
                if (rs.getString("identity").equals("SUPERUSER") && rs.getLong("mid") != (rs2.getMid())) return false;
                ps2.setLong(1, mid);
                ps2.setLong(2, mid);
                ps2.setLong(3, mid);
                ps2.setLong(4, mid);
                ps2.setLong(5, mid);
                ps2.setString(6, "like");
                ps2.setLong(7, mid);
                ps2.setString(8, "coin");
                ps2.setLong(9, mid);
                ps2.setString(10, "collect");
                ps2.execute();
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        if (auth == null) {
            return false;
        }
        ResultSet rs=null;
        ResultSet rs22=null;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement("SELECT 1 FROM Users WHERE mid = ?");
             PreparedStatement ps2 = connection.prepareStatement("SELECT 1 FROM FollRela WHERE followerMid = ? AND followeeMid = ? ;");
        ) {UserRecord rs2=isValidAuth(connection,auth);
            if(rs2==null)return false;
            if(rs2.getMid()==followeeMid)return false;
            ps.setLong(1, followeeMid);
            rs = ps.executeQuery();
            if(!rs.next())return false;//followeeMid不存在
            ps2.setLong(1,rs2.getMid());
            ps2.setLong(2,followeeMid);
            rs22=ps2.executeQuery();
            if(!rs22.next()){
                try(PreparedStatement ps3=connection.prepareStatement("INSERT INTO FollRela (followerMid,followeeMid) VALUES (?,?);")){
                    ps3.setLong(1,rs2.getMid());
                    ps3.setLong(2,followeeMid);
                    if(ps3.executeUpdate()==1)return true;else return false;
                }
            }else {
                try(PreparedStatement ps3=connection.prepareStatement("DELETE FROM FollRela WHERE followerMid = ? AND followeeMid = ? ;")){
                    ps3.setLong(1,rs2.getMid());
                    ps3.setLong(2,followeeMid);
                    if(ps3.executeUpdate()==1)return true;else return false;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (rs22 != null) {
                try {
                    rs22.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        try (Connection connection = dataSource.getConnection();) {
            UserInfoResp userInfoResp=new UserInfoResp();
            try(PreparedStatement ps = connection.prepareStatement(
                    "SELECT coin FROM Users WHERE mid = ?");){
                ps.setLong(1,mid);
                try(ResultSet rs = ps.executeQuery()){
                    if(rs.next()){
                        userInfoResp.setCoin(rs.getInt("coin"));
                        userInfoResp.setMid(mid);
                    }
                }
            }
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT followerMid FROM FollRela WHERE followeeMid = ?");
                 PreparedStatement ps2 = connection.prepareStatement(
                         "SELECT followeeMid FROM FollRela WHERE followerMid = ?");
                 PreparedStatement ps3 = connection.prepareStatement(
                         "SELECT bv FROM ViewRela WHERE viewerMid = ?");
                 PreparedStatement ps4 = connection.prepareStatement(
                         "SELECT bv FROM Videos WHERE ownerMid = ?");
                 PreparedStatement ps5 = connection.prepareStatement(
                         "SELECT bv FROM InteractionRela WHERE mid = ? AND behavior = ?");
            ) {

                ps.setLong(1, mid);
                try (ResultSet rs = ps.executeQuery()) {
                    ArrayList<Long> followerMids = new ArrayList<>();
                    while (rs.next()) {
                        long followerMid = rs.getLong("followerMid");
                        followerMids.add(followerMid);
                    }
                    if (!followerMids.isEmpty()) {
                        long[] followerMidsArray = new long[followerMids.size()];
                        for (int i = 0; i < followerMids.size(); i++) {
                            followerMidsArray[i] = followerMids.get(i);
                        }
                        userInfoResp.setFollower(followerMidsArray);
                    } else {
                        userInfoResp.setFollower(new long[0]);
                    }
                }

                ps2.setLong(1, mid);
                try (ResultSet rs = ps2.executeQuery()) {
                    ArrayList<Long> followerMids = new ArrayList<>();
                    while (rs.next()) {
                        long followerMid = rs.getLong("followeeMid");
                        followerMids.add(followerMid);
                    }
                    if (!followerMids.isEmpty()) {
                        long[] followerMidsArray = new long[followerMids.size()];
                        for (int i = 0; i < followerMids.size(); i++) {
                            followerMidsArray[i] = followerMids.get(i);
                        }
                        userInfoResp.setFollowing(followerMidsArray);
                    } else {
                        userInfoResp.setFollowing(new long[0]);
                    }
                }

                ps3.setLong(1,mid);
                try (ResultSet rs3 = ps3.executeQuery()) {
                    ArrayList<String> temp = new ArrayList<>();
                    while (rs3.next()) {
                        String bv = rs3.getString("bv");
                        temp.add(bv);
                    }
                    String[] temp2 = temp.toArray(new String[temp.size()]);
                    userInfoResp.setWatched(temp2);
                }

                ps4.setLong(1,mid);
                try (ResultSet rs4 = ps4.executeQuery()) {
                    ArrayList<String> temp = new ArrayList<>();
                    while (rs4.next()) {
                        String bv = rs4.getString("bv");
                        temp.add(bv);
                    }
                    String[] temp2 = temp.toArray(new String[temp.size()]);
                    userInfoResp.setPosted(temp2);
                }

                ps5.setLong(1,mid);
                ps5.setString(2,"like");
                try (ResultSet rs5 = ps5.executeQuery()) {
                    ArrayList<String> temp = new ArrayList<>();
                    while (rs5.next()) {
                        String bv = rs5.getString("bv");
                        temp.add(bv);
                    }
                    String[] temp2 = temp.toArray(new String[temp.size()]);
                    userInfoResp.setLiked(temp2);
                }

                ps5.setLong(1,mid);
                ps5.setString(2,"collect");
                try (ResultSet rs5 = ps5.executeQuery()) {
                    ArrayList<String> temp = new ArrayList<>();
                    while (rs5.next()) {
                        String bv = rs5.getString("bv");
                        temp.add(bv);
                    }
                    String[] temp2 = temp.toArray(new String[temp.size()]);
                    userInfoResp.setCollected(temp2);
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
            return userInfoResp;
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    public boolean watchVideos(AuthInfo auth, String bv,float watchTime) {
        try(Connection connection= dataSource.getConnection()){
            UserRecord userRecord=isValidAuth(connection,auth);
            if(userRecord==null)return false;
            if(canBvReach(connection,bv,userRecord)){
                if(!isView(connection,bv,userRecord)){
                    try(PreparedStatement ps=connection.prepareStatement("INSERT INTO ViewRela (viewTime,bv,viewerMid) VALUES (?,?,?);UPDATE Videos SET watchTime = watchTime + ? WHERE bv = ?;")){
                        ps.setFloat(1,watchTime);
                        ps.setString(2,bv);
                        ps.setLong(3,userRecord.getMid());
                        ps.setDouble(4,(double)watchTime);
                        ps.setString(5,bv);
                        ps.execute();
                        return true;
                    }
                }else {
                    try(PreparedStatement ps=connection.prepareStatement("UPDATE ViewRela SET viewTime = ? WHERE bv = ? AND viewerMid = ?;UPDATE Videos SET watchTime = watchTime + ? WHERE bv = ?;")){
                        ps.setFloat(1,watchTime);
                        ps.setString(2,bv);
                        ps.setLong(3,userRecord.getMid());
                        ps.setDouble(4,(double)watchTime);
                        ps.setString(5,bv);
                        ps.execute();
                        return true;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
}
