package io.sustc.service.impl;
import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

import static io.sustc.service.impl.Utils.*;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;
    public boolean isReqValid(Connection connection,PostVideoReq req,long ownermid,String bv){
        if(req.getPublicTime()==null)return false;
        if(req.getTitle()==null||req.getTitle().isEmpty()||req.getDuration()<10)return false;
        LocalDateTime publicTimeAsLocalDateTime = req.getPublicTime().toLocalDateTime();
        LocalDateTime now = LocalDateTime.now();
        if (publicTimeAsLocalDateTime.isBefore(now)) {
            return false;
        }

        if(bv==null) {
            try (PreparedStatement ps = connection.prepareStatement("SELECT 1 FROM Videos WHERE title = ? AND ownerMid = ?;")) {//AND ownerMid = ?
                ps.setString(1, req.getTitle());
                ps.setLong(2, ownermid);
                return !ps.executeQuery().next();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else {
            try(PreparedStatement ps = connection.prepareStatement("SELECT 1 FROM Videos WHERE title = ? AND ownerMid = ? AND bv != ?;")){
                ps.setString(1,req.getTitle());
                ps.setLong(2,ownermid);
                ps.setString(3,bv);
                return !ps.executeQuery().next();
            } catch (SQLException e){
                e.printStackTrace();
            }
        }
        return true;
    }
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        if (auth == null) {
            return null;
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "INSERT INTO Videos (title, ownerMid, ownerName, commitTime, publicTime, duration,description) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING bv")) {
            UserRecord rs1=isValidAuth(connection,auth);
            if(rs1==null)return null;
            if(!isReqValid(connection,req,rs1.getMid(),null))return null;

            ps.setString(1, req.getTitle());
            ps.setLong(2, rs1.getMid());
            ps.setString(3, rs1.getName());
            ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            ps.setTimestamp(5, req.getPublicTime());
            ps.setFloat(6, req.getDuration());
            ps.setString(7, req.getDescription());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String generatedBv = rs.getString(1);
                    return generatedBv;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        if (auth == null) {
            return false;
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement("SELECT ownerMid FROM Videos WHERE bv = ? ;");
             PreparedStatement ps2 = connection.prepareStatement("DELETE FROM Videos WHERE bv = ? ;");
        ) {UserRecord rs2=isValidAuth(connection,auth);
            if(rs2==null)return false;
            ps.setString(1, bv);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
                //user删别人
            if (String.valueOf(rs2.getIdentity()).equals("USER") && rs.getLong("ownerMid") != (rs2.getMid())){return false;}
            ps2.setString(1,bv);
            ps2.executeUpdate();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        if(auth == null)return false;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement("SELECT ownerMid,duration,publicTime,title,description,reviewTime FROM Videos WHERE bv = ? ;");
             PreparedStatement ps2 = connection.prepareStatement("UPDATE Videos SET title = ?, publicTime = ?, description = ?, commitTime = ?, reviewTime = NULL, reviewer = NULL WHERE bv = ?");
        ) {UserRecord rs1=isValidAuth(connection,auth);
            if(rs1==null)return false;
            if(!isReqValid(connection,req,rs1.getMid(),bv))return false;
            ps.setString(1, bv);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()){return false;}
            if (rs.getLong("ownerMid")!=(rs1.getMid())){return false;}
            if(req.getDuration()!=rs.getFloat("duration")){return false;}
            if(rs.getTimestamp("publicTime")==null||!rs.getTimestamp("publicTime").equals(req.getPublicTime()));else {
                if(!req.getTitle().equals(rs.getString("title")));else {
                    if (req.getDescription() == null && rs.getString("description") == null) return false;
                    else {
                        if (req.getDescription() == null || rs.getString("description") == null) ;
                        else {
                            if (!req.getDescription().equals(rs.getString("description"))) ;
                            else return false;
                        }
                    }
                }
            }

            ps2.setString(1,req.getTitle());
            ps2.setTimestamp(2,req.getPublicTime());
            ps2.setString(3,req.getDescription());
            ps2.setTimestamp(4,Timestamp.valueOf(LocalDateTime.now()));
            ps2.setString(5,bv);
            ps2.executeUpdate();
            if(rs.getTimestamp("reviewTime")==null)return false;
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {

        if (keywords == null || keywords.isEmpty() || pageSize <= 0 || pageNum <= 0) {
            return null;
        }
        if(auth==null)return null;

        try (Connection connection = dataSource.getConnection()) {
            UserRecord rs1=isValidAuth(connection,auth);
            List<String> videoBVs = new ArrayList<>();
            if(rs1==null)return null;
            if(String.valueOf(rs1.getIdentity()).equals("SUPERUSER")) {
                try(PreparedStatement ps = connection.prepareStatement("SELECT bv, relevance\n" +
                        "FROM (\n" +
                        "         SELECT bv,\n" +
                        "                (count_non_overlapping_substrings(title, ?) +\n" +
                        "                 count_non_overlapping_substrings(ownerName, ?) +\n" +
                        "                 count_non_overlapping_substrings(description, ?)) AS relevance,viewCount\n" +
                        "         FROM Videos\n" +
                        "     ) AS sub\n" +
                        "WHERE sub.relevance > 0\n" +
                        "ORDER BY relevance DESC, viewCount DESC\n" +
                        "LIMIT ? OFFSET ?;")) {
                    ps.setString(1,keywords);
                    ps.setString(2,keywords);
                    ps.setString(3,keywords);
                    ps.setInt(4, pageSize);
                    ps.setInt(5, (pageNum - 1) * pageSize);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            videoBVs.add(rs.getString("bv"));
                        }
                    }
                    return videoBVs;
                }
            }else {
                try(PreparedStatement ps = connection.prepareStatement("SELECT bv, relevance\n" +
                        "FROM (\n" +
                        "         SELECT bv,\n" +
                        "                (count_non_overlapping_substrings(title, ?) +\n" +
                        "                 count_non_overlapping_substrings(ownerName, ?) +\n" +
                        "                 count_non_overlapping_substrings(description, ?)) AS relevance,viewCount\n" +
                        "         FROM Videos\n" +
                        "         WHERE  ownerMid = ? OR(reviewTime is not null AND publicTime<now())\n" +
                        "     ) AS sub\n" +
                        "WHERE sub.relevance > 0\n" +
                        "ORDER BY relevance DESC, viewCount DESC\n" +
                        "LIMIT ? OFFSET ?;")) {
                    ps.setString(1,keywords);
                    ps.setString(2,keywords);
                    ps.setString(3,keywords);
                    ps.setLong(4,rs1.getMid());
                    ps.setInt(5, pageSize);
                    ps.setInt(6, (pageNum - 1) * pageSize);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            videoBVs.add(rs.getString("bv"));
                        }
                    }
                    return videoBVs;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;

    }



    @Override
    public double getAverageViewRate(String bv) {
        try(Connection connection=dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("SELECT watchTime,viewCount,duration FROM Videos WHERE bv = ? ;");
        ) {ps.setString(1,bv);
            try(ResultSet rs=ps.executeQuery()){
                if(rs.next()){
                    return rs.getDouble("watchTime")/(double) rs.getLong("viewCount")/(double) rs.getFloat("duration");
                }else return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        Set<Integer> hotspot = new HashSet<>();
        try(Connection connection = dataSource.getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT\n" +
                "  FLOOR(time / 10) AS chunk_index,\n" +
                "  COUNT(*) AS danmu_count\n" +
                "FROM Danmu\n" +
                "WHERE bv = ? \n" +
                "GROUP BY chunk_index\n" +
                "ORDER BY danmu_count DESC;")
        ){ps.setString(1,bv);
            ResultSet rs=ps.executeQuery();
            int index = 0;
            long count = 0;
            while (rs.next()){
                if(count!=0&&count>rs.getLong("danmu_count"))break;
                index=rs.getInt("chunk_index");
                count=rs.getLong("danmu_count");
                if(count==0) break;
                hotspot.add(index);
            }
            return hotspot;
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        try(Connection connection=dataSource.getConnection();
            PreparedStatement ps2 = connection.prepareStatement("SELECT ownerMid,reviewTime FROM Videos WHERE bv = ? ;")
        ){UserRecord rs = isValidAuth(connection,auth);
            if (rs==null)return false;
            if(!String.valueOf(rs.getIdentity()).equals("SUPERUSER"))return false;
            ps2.setString(1,bv);
            try(ResultSet rs2 = ps2.executeQuery()) {
                if (rs2.next()) {
                    if (rs2.getTimestamp("reviewTime") != null) return false;
                    if (rs2.getLong("ownerMid") == (auth.getMid())) return false;
                    try (PreparedStatement ps3 = connection.prepareStatement("UPDATE Videos SET reviewTime = ?, reviewer = ? WHERE bv = ?")) {
                        ps3.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
                        ps3.setLong(2, auth.getMid());
                        ps3.setString(3, bv);
                        if (ps3.executeUpdate() == 1) return true;
                        else return false;
                    }
                } else return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        try(Connection connection= dataSource.getConnection();){
            UserRecord userRecord=isValidAuth(connection,auth);
            if(userRecord==null)return false;
            if(isDone(connection,bv, userRecord.getMid(), "coin"))return false;
            if(canBvReach(connection,bv,userRecord)){
                if(userRecord.getCoin()<1)return false;
                try(PreparedStatement ps=connection.prepareStatement("INSERT INTO InteractionRela (mid,bv,behavior) VALUES (?,?,?);UPDATE Users SET coin=coin-1 where mid = ?;UPDATE Videos SET coinCount=coinCount+1 where bv = ?;");
                ){
                    ps.setString(5,bv);
                    ps.setLong(1,userRecord.getMid());
                    ps.setString(2,bv);
                    ps.setString(3,"coin");
                    ps.setLong(4,userRecord.getMid());
                    ps.execute();
                    return true;
                }
            }else return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        try(Connection connection= dataSource.getConnection();){
            UserRecord userRecord=isValidAuth(connection,auth);
            if(userRecord==null)return false;
            if(canBvReach(connection,bv,userRecord)){
                if(isDone(connection,bv, userRecord.getMid(), "like")){
                    try (PreparedStatement ps = connection.prepareStatement("DELETE FROM InteractionRela WHERE mid = ? AND bv = ? AND behavior = ?;UPDATE Videos SET likeCount=likeCount-1 where bv = ?;");
                    ) {ps.setString(4,bv);
                        ps.setLong(1,userRecord.getMid());
                        ps.setString(2,bv);
                        ps.setString(3,"like");
                        ps.executeUpdate();
                        return false;
                    }
                }else {
                    try (PreparedStatement ps = connection.prepareStatement("INSERT INTO InteractionRela (mid,bv,behavior) VALUES (?,?,?);UPDATE Videos SET likeCount=likeCount+1 where bv = ?;");
                         PreparedStatement ps2 = connection.prepareStatement("")
                    ) {ps.setString(4,bv);
                        ps.setLong(1, userRecord.getMid());
                        ps.setString(2, bv);
                        ps.setString(3, "like");
                        ps.executeUpdate();
                        return true;
                    }
                }
            }else return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        try(Connection connection= dataSource.getConnection();){
            UserRecord userRecord=isValidAuth(connection,auth);
            if(userRecord==null)return false;
            if(canBvReach(connection,bv,userRecord)){
                if(isDone(connection,bv, userRecord.getMid(), "collect")){
                    try (PreparedStatement ps = connection.prepareStatement("DELETE FROM InteractionRela WHERE mid = ? AND bv = ? AND behavior = ?;UPDATE Videos SET collectCount=collectCount-1 where bv = ?;");
                    ) {ps.setString(4,bv);
                        ps.setLong(1,userRecord.getMid());
                        ps.setString(2,bv);
                        ps.setString(3,"collect");
                        ps.executeUpdate();
                        return false;
                    }
                }else {
                    try (PreparedStatement ps = connection.prepareStatement(
                            "INSERT INTO InteractionRela (mid,bv,behavior) VALUES (?,?,?);UPDATE Videos SET collectCount=collectCount+1 where bv = ?;");
                    ) {ps.setString(4,bv);
                        ps.setLong(1, userRecord.getMid());
                        ps.setString(2, bv);
                        ps.setString(3, "collect");
                        ps.executeUpdate();
                        return true;
                    }
                }
            }else return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}