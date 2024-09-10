package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserRecord;

import java.lang.constant.Constable;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;

public class Utils {


    public static boolean canBvReach(Connection connection,String bv,UserRecord userRecord){
        if (bv==null||bv.isEmpty())return false;
        try(PreparedStatement ps = connection.prepareStatement(
                "SELECT ownerMid,reviewTime,publicTime FROM Videos WHERE bv = ?")){
                ps.setString(1,bv);
                try(ResultSet rs1 = ps.executeQuery()) {
                    if (rs1.next()) {
                        if(userRecord.getMid() == rs1.getLong("ownerMid"))return false;
                        if (String.valueOf(userRecord.getIdentity()).equals("SUPERUSER")) return true;
                        if (rs1.getTimestamp("reviewTime") == null) return false;
                        if(rs1.getTimestamp("publicTime")==null)return true;
                        if (rs1.getTimestamp("publicTime") != null && rs1.getTimestamp("publicTime").compareTo(Timestamp.valueOf(LocalDateTime.now())) > 0)
                            return false;
                    } else return false;
                    return true;
                }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }
    public static boolean isValidDate(String dateString) {
        String[] parts = dateString.split("月|日");
        if (parts.length != 2) {
            return false;
        }
        try {
            int month = Integer.parseInt(parts[0]);
            int day = Integer.parseInt(parts[1]);

            if (month < 1 || month > 12) {
                return false;
            }
            int[] daysInMonth = { 0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
            int maxDay = daysInMonth[month];
            return day >= 1 && day <= maxDay;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean videoExists(Connection connection,String bv) {
        String sql = "SELECT 1 FROM Videos WHERE bv = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, bv);
            return ps.executeQuery().next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }
    public static byte isBvOwner(Connection connection,String bv,long mid){//0 not found,1 yes,2 no
        try(PreparedStatement ps = connection.prepareStatement(
                "SELECT ownerMid FROM Videos WHERE bv = ?")){
            ps.setString(1,bv);
            try(ResultSet rs1 = ps.executeQuery();) {
                if (rs1.next()) {
                    if (mid == rs1.getLong("ownerMid")) return 1;else return 2;
                } else return 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static boolean isDone(Connection connection,String bv,long mid,String behavior){
        try(PreparedStatement ps = connection.prepareStatement(
                "SELECT 1 FROM InteractionRela WHERE mid = ? AND bv = ? AND behavior = ?")){
            ps.setLong(1,mid);
            ps.setString(2,bv);
            ps.setString(3,behavior);
            try(ResultSet rs1 = ps.executeQuery()) {
                return rs1.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    public static boolean isPublic(Connection connection,String bv,float time){
        try(PreparedStatement ps = connection.prepareStatement(
                "SELECT reviewTime,publicTime,duration FROM Videos WHERE bv = ?")){
            ps.setString(1,bv);
            try(ResultSet rs1=ps.executeQuery();) {
                if (rs1.next()) {
                    if (rs1.getTimestamp("reviewTime") == null) return false;
                    if (rs1.getTimestamp("publicTime") != null && rs1.getTimestamp("publicTime").compareTo(Timestamp.valueOf(LocalDateTime.now())) > 0)
                        return false;
                    if(time!=-1&&time>rs1.getFloat("duration"))return false;
                } else return false;
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean isView(Connection connection,String bv,UserRecord userRecord){
        try(PreparedStatement ps = connection.prepareStatement(
                "SELECT 1 FROM ViewRela WHERE bv = ? AND viewerMid = ?;")){
            ps.setString(1,bv);
            ps.setLong(2,userRecord.getMid());
            try(ResultSet rs1 = ps.executeQuery();) {
                return rs1.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }




    public static String encodeToBase64(long[] longArray) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(longArray.length * Long.BYTES);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        longBuffer.put(longArray);

        return Base64.getEncoder().encodeToString(byteBuffer.array());
    }

    public static long[] decodeFromBase64(String base64String) {
        byte[] bytes = Base64.getDecoder().decode(base64String);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        long[] longArray = new long[bytes.length / Long.BYTES];
        longBuffer.get(longArray);
        return longArray;
    }

    public static String modifyBase64String(String base64String, long m) {
        // 解码 Base64 字符串
        byte[] bytes = Base64.getDecoder().decode(base64String);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        // 将字节转换为 long 数组
        ArrayList<Long> longList = new ArrayList<>();
        while (longBuffer.hasRemaining()) {
            longList.add(longBuffer.get());
        }
        // 检查 m 是否存在，并相应地修改数组
        if (longList.contains(m)) {
            longList.remove(m);
        } else {
            longList.add(m);
        }
        // 创建新的 ByteBuffer 并重新编码为 Base64
        ByteBuffer newByteBuffer = ByteBuffer.allocate(longList.size() * Long.BYTES);
        LongBuffer newLongBuffer = newByteBuffer.asLongBuffer();
        for (Long l : longList) {
            newLongBuffer.put(l);
        }
        return Base64.getEncoder().encodeToString(newByteBuffer.array());
    }

    public static boolean isExist(Connection connection,String table_name,String rule) {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT 1 FROM ? WHERE ?")) {
            ps.setString(1, table_name);
            ps.setString(2, rule);
            ResultSet rs = ps.executeQuery();
            boolean hasRow = rs.next();
            rs.close();
            return hasRow;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static UserRecord isValidAuth(Connection connection, AuthInfo auth) {
        if(auth.getPassword()!=null){
            if(auth.getMid()==0)return null;
            try (PreparedStatement ps = connection.prepareStatement("SELECT password,mid,name,coin,identity FROM Users WHERE mid = ?")) {
                ps.setLong(1, auth.getMid());
                try(ResultSet rs = ps.executeQuery();) {
                    if (!rs.next()) return null;
                    if (!rs.getString("password").equals(auth.getPassword())) return null;
                    UserRecord userRecord=new UserRecord();
                    userRecord.setMid(rs.getLong("mid"));
                    userRecord.setName(rs.getString("name"));
                    userRecord.setIdentity(UserRecord.Identity.valueOf(rs.getString("identity")));
                    userRecord.setCoin(rs.getInt("coin"));
                    return userRecord;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else {
            boolean isQqEmpty = (auth.getQq() == null) || auth.getQq().isEmpty();
            boolean isWechatEmpty = (auth.getWechat() == null) || auth.getWechat().isEmpty();
            if(!isQqEmpty&&!isWechatEmpty){
                try (PreparedStatement ps = connection.prepareStatement("SELECT password,mid,name,coin,identity FROM Users WHERE qq = ? AND wechat = ?")) {
                    ps.setString(1, auth.getQq());
                    ps.setString(2, auth.getWechat());
                    try(ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) return null;
                        UserRecord userRecord=new UserRecord();
                        userRecord.setMid(rs.getLong("mid"));
                        userRecord.setName(rs.getString("name"));
                        userRecord.setIdentity(UserRecord.Identity.valueOf(rs.getString("identity")));
                        userRecord.setCoin(rs.getInt("coin"));
                        return userRecord;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else if (!isQqEmpty) {
                try (PreparedStatement ps = connection.prepareStatement("SELECT password,mid,name,coin,identity FROM Users WHERE qq = ?")) {
                    ps.setString(1, auth.getQq());
                    try(ResultSet rs = ps.executeQuery();) {
                        if (!rs.next()) return null;
                        UserRecord userRecord=new UserRecord();
                        userRecord.setMid(rs.getLong("mid"));
                        userRecord.setName(rs.getString("name"));
                        userRecord.setIdentity(UserRecord.Identity.valueOf(rs.getString("identity")));
                        userRecord.setCoin(rs.getInt("coin"));
                        return userRecord;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }else if(!isWechatEmpty){
                try (PreparedStatement ps = connection.prepareStatement("SELECT password,mid,name,coin,identity FROM Users WHERE wechat = ?")) {
                    ps.setString(1, auth.getWechat());
                    try(ResultSet rs = ps.executeQuery();) {
                        if (!rs.next()) return null;
                        UserRecord userRecord=new UserRecord();
                        userRecord.setMid(rs.getLong("mid"));
                        userRecord.setName(rs.getString("name"));
                        userRecord.setIdentity(UserRecord.Identity.valueOf(rs.getString("identity")));
                        userRecord.setCoin(rs.getInt("coin"));
                        return userRecord;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }else {
                return null;
            }
        }
        return null;

        /*
        boolean isPasswordEmpty = (auth.getPassword() == null) || auth.getPassword().isEmpty();
        UserRecord userRecord=new UserRecord();
        if(!isWechatEmpty||!isQqEmpty){
            ResultSet rs1=null;
            ResultSet rs2=null;
            try(PreparedStatement ps1 = connection.prepareStatement("SELECT mid,name,coin,identity FROM Users WHERE wechat = ?");
                PreparedStatement ps2 = connection.prepareStatement("SELECT mid,name,coin,identity FROM Users WHERE qq = ?");
                ) {
                long mid1 = 0;
                long mid2 = 0;
                String wechat=auth.getWechat();
                String qq=auth.getQq();
                boolean ismid1=false;
                boolean ismid2=false;
                if(isWechatEmpty){wechat="No need to do";}
                if(isQqEmpty){qq="No need to do";}
                ps1.setString(1,wechat);
                ps2.setString(1,qq);
                rs1 = ps1.executeQuery();
                if (!rs1.next()) {
                    ismid1=false;
                }else {
                    mid1 = rs1.getLong("mid");
                    ismid1=true;
                }

                rs2 = ps2.executeQuery();
                if (!rs2.next()) {
                    ismid2=false;
                }else {
                    mid2 = rs2.getLong("mid");
                    ismid2=true;
                }

                if(!ismid1&&!ismid2){
                    return null;
                }else if(!ismid1){
                    userRecord.setMid(rs2.getLong("mid"));
                    userRecord.setName(rs2.getString("name"));
                    userRecord.setIdentity(UserRecord.Identity.valueOf(rs2.getString("identity")));
                    userRecord.setCoin(rs2.getInt("coin"));
                    return userRecord;
                }else if(!ismid2){
                    userRecord.setMid(rs1.getLong("mid"));
                    userRecord.setName(rs1.getString("name"));
                    userRecord.setIdentity(UserRecord.Identity.valueOf(rs1.getString("identity")));
                    userRecord.setCoin(rs1.getInt("coin"));
                    return userRecord;
                }else {
                    if(mid1==mid2){userRecord.setMid(rs1.getLong("mid"));
                        userRecord.setName(rs1.getString("name"));
                        userRecord.setIdentity(UserRecord.Identity.valueOf(rs1.getString("identity")));
                        userRecord.setCoin(rs1.getInt("coin"));
                        return userRecord;}else return null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                if (rs1 != null) {
                    try {
                        rs1.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if (rs2 != null) {
                    try {
                        rs2.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }else {
            if(auth.getMid()!=0&&!isPasswordEmpty){
                try (PreparedStatement ps = connection.prepareStatement("SELECT password,mid,name,coin,identity FROM Users WHERE mid = ?")) {
                    ps.setLong(1, auth.getMid());
                    try(ResultSet rs = ps.executeQuery();) {
                        if (!rs.next()) return null;
                        String password = rs.getString("password");
                        if (!password.equals(auth.getPassword())) return null;
                        userRecord.setMid(rs.getLong("mid"));
                        userRecord.setName(rs.getString("name"));
                        userRecord.setIdentity(UserRecord.Identity.valueOf(rs.getString("identity")));
                        userRecord.setCoin(rs.getInt("coin"));
                        return userRecord;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }else return null;
        }
        return null;

         */
    }

    public static String floatArrayToBase64String(float[] array) {
        ByteBuffer buffer = ByteBuffer.allocate(array.length * 4);
        for (float value : array) {
            buffer.putFloat(value);
        }
        return Base64.getEncoder().encodeToString(buffer.array());
    }

    public static float[] base64StringToFloatArray(String base64String) {
        byte[] bytes = Base64.getDecoder().decode(base64String);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        float[] array = new float[bytes.length / 4];
        for (int i = 0; i < array.length; i++) {
            array[i] = buffer.getFloat();
        }
        return array;
    }

    public static float calculateAverage(float[] array) {
        float sum = 0;
        for (float value : array) {
            sum += value;
        }
        return array.length > 0 ? sum / array.length : 0;
    }

    private static String encryptDecryptXOR(String input, String key) {
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            output.append((char) (input.charAt(i) ^ key.charAt(i % key.length())));
        }
        return output.toString();
    }

    // 加密函数，使用 XOR 加密后转为 Base64
    public static String encrypt(String input, String key) {
        String encrypted = encryptDecryptXOR(input, key);
        return Base64.getEncoder().encodeToString(encrypted.getBytes());
    }

    // 解密函数，先将 Base64 解码，然后使用 XOR 解密
    public static String decrypt(String input, String key) throws Exception {
        byte[] decodedBytes = Base64.getDecoder().decode(input);
        return encryptDecryptXOR(new String(decodedBytes), key);
    }

}
