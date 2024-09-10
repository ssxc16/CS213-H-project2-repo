package io.sustc.service.impl;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.nio.LongBuffer;
import java.util.ArrayList;
import io.sustc.dto.*;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import io.sustc.service.impl.Utils.*;

import static io.sustc.service.impl.Utils.isValidAuth;
import static io.sustc.service.impl.Utils.videoExists;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {

    @Autowired
    private DataSource dataSource;
    @Override
    public List<String> recommendNextVideo(String bv) {
        ResultSet rs=null;
        try(Connection connection =dataSource.getConnection()) {
            if (!videoExists(connection,bv)) {
                return null;
            }
            String sql = "SELECT v.bv \n" +
                    "FROM (\n" +
                    "    SELECT vr1.bv\n" +
                    "    FROM ViewRela vr1\n" +
                    "    INNER JOIN ViewRela vr2 ON vr1.viewerMid = vr2.viewerMid\n" +
                    "    WHERE vr2.bv = ? AND vr1.bv <> ?\n" +
                    "    GROUP BY vr1.bv\n" +
                    "    ORDER BY COUNT(vr1.viewerMid) DESC\n" +
                    "    LIMIT 5\n" +
                    ") AS subquery\n" +
                    "INNER JOIN Videos v ON subquery.bv = v.bv\n" +
                    "ORDER BY (SELECT COUNT(*)\n" +
                    "          FROM ViewRela vr3\n" +
                    "          WHERE vr3.bv = subquery.bv\n" +
                    "          AND vr3.viewerMid IN (SELECT viewerMid FROM ViewRela vr4 WHERE vr4.bv = ?))\n" +
                    "          DESC, v.bv ASC;\n";

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, bv);
                ps.setString(2, bv);
                ps.setString(3, bv);
                rs = ps.executeQuery();
                List<String> similarVideos = new ArrayList<>();

                while (rs.next()) {
                    similarVideos.add(rs.getString("bv"));
                }

                return similarVideos;
            } catch (SQLException e) {
                e.printStackTrace();
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
        }
        return null;
    }
    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if (pageSize <= 0 || pageNum <= 0) return null;
        List<String> recommendedVideos = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement("WITH VideoStats AS (\n" +
                     "    SELECT\n" +
                     "        v.bv,\n" +
                     "        (LEAST(v.coinCount::DOUBLE PRECISION/v.viewCount,1)+LEAST(v.likeCount::DOUBLE PRECISION/v.viewCount,1)+LEAST(v.collectCount::DOUBLE PRECISION/v.viewCount,1)) AS interaction_rate,\n" +
                     "        v.danmuCount::DOUBLE PRECISION/v.viewCount AS danmu_avg,\n" +
                     "        v.watchTime/v.viewCount/v.duration AS finish_avg\n" +
                     "    FROM Videos v\n" +
                     "    group by v.bv)\n" +
                     "SELECT bv,interaction_rate,danmu_avg,finish_avg\n" +
                     "FROM VideoStats\n" +
                     "ORDER BY (interaction_rate + danmu_avg + finish_avg) DESC\n" +
                     "LIMIT ? OFFSET ?;")) {

            // 设置分页参数
            pstmt.setInt(1, pageSize);
            pstmt.setInt(2, (pageNum - 1) * pageSize);
            try(ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    recommendedVideos.add(rs.getString("bv"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return recommendedVideos;
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        if(auth==null)return null;
        if (pageSize <= 0 || pageNum <= 0) return null;
        List<String> recommend=new ArrayList<>();
        try(Connection connection = dataSource.getConnection()){
            UserRecord userRecord = isValidAuth(connection,auth);
            if(userRecord==null)return null;
            try(PreparedStatement ps2 =connection.prepareStatement("SELECT EXISTS (\n" +
                    "    SELECT 1\n" +
                    "    FROM ViewRela AS V\n" +
                    "             JOIN Videos AS Vi ON V.bv = Vi.bv\n" +
                    "             JOIN Users AS U ON Vi.ownerMid = U.mid\n" +
                    "             JOIN (SELECT followeeMid \n" +
                    "                   FROM FollRela \n" +
                    "                   WHERE followerMid = ? \n" +
                    "                   INTERSECT \n" +
                    "                   SELECT followerMid \n" +
                    "                   FROM FollRela \n" +
                    "                   WHERE followeeMid = ?) AS Friends\n" +
                    "                  ON V.viewerMid = Friends.followeeMid\n" +
                    "             LEFT JOIN ViewRela AS UserViews \n" +
                    "                  ON V.bv = UserViews.bv AND UserViews.viewerMid = ?\n" +
                    "    WHERE UserViews.bv IS NULL\n" +
                    "    GROUP BY V.bv, U.level, Vi.publicTime\n" +
                    ") AS HasRecords;\n") ;

                    PreparedStatement ps = connection.prepareStatement("SELECT V.bv, COUNT(DISTINCT V.viewerMid) AS FriendsCount, U.level, Vi.publicTime\n" +
                    "FROM ViewRela AS V\n" +
                    "         JOIN Videos AS Vi ON V.bv = Vi.bv\n" +
                    "         JOIN Users AS U ON Vi.ownerMid = U.mid\n" +
                    "         JOIN (SELECT followeeMid FROM FollRela WHERE followerMid = ? INTERSECT SELECT followerMid FROM FollRela WHERE followeeMid = ?) AS Friends\n" +
                    "              ON V.viewerMid = Friends.followeeMid\n" +
                    "         LEFT JOIN ViewRela AS UserViews ON V.bv = UserViews.bv AND UserViews.viewerMid = ?\n" +
                    "WHERE UserViews.bv IS NULL\n" +
                    "GROUP BY V.bv, U.level, Vi.publicTime\n" +
                    "ORDER BY FriendsCount DESC, U.level DESC, Vi.publicTime DESC\n" +
                    "LIMIT ? OFFSET ?;")){
                ps2.setLong(1,userRecord.getMid());
                ps2.setLong(2,userRecord.getMid());
                ps2.setLong(3,userRecord.getMid());
                try (ResultSet resultSet = ps2.executeQuery()) {
                    if (resultSet.next()) {
                        boolean hasRecords = resultSet.getBoolean("HasRecords");
                        if (!hasRecords) {
                            connection.close();
                            return generalRecommendations(pageSize,pageNum);
                        }
                    }
                }
                ps.setLong(1,userRecord.getMid());
                ps.setLong(2,userRecord.getMid());
                ps.setLong(3,userRecord.getMid());
                ps.setInt(4, pageSize);
                ps.setInt(5, (pageNum - 1) * pageSize);
                try(ResultSet rs = ps.executeQuery();) {
                    while (rs.next()) {
                        recommend.add(rs.getString("bv"));
                    }
                    return recommend;
                }
            }
        }catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        if(auth==null)return null;
        if (pageSize <= 0 || pageNum <= 0) return null;
        List<Long> recommend=new ArrayList<>();
        try(Connection connection = dataSource.getConnection()){
            UserRecord userRecord = isValidAuth(connection,auth);
            if(userRecord==null)return null;
            try(PreparedStatement ps = connection.prepareStatement("WITH UserFollowings AS (\n" +
                    "    SELECT followeeMid\n" +
                    "    FROM FollRela\n" +
                    "    WHERE followerMid = ?\n" +
                    "),\n" +
                    "     CommonFollowings AS (\n" +
                    "         SELECT followerMid\n" +
                    "         FROM FollRela\n" +
                    "         WHERE followeeMid IN (SELECT followeeMid FROM UserFollowings)\n" +
                    "           AND followerMid != ?\n" +
                    "     ),\n" +
                    "     RecommendedUsers AS (\n" +
                    "         SELECT u.mid, u.level, COUNT(cf.followerMid) AS common_followings\n" +
                    "         FROM Users u\n" +
                    "                  INNER JOIN CommonFollowings cf ON u.mid = cf.followerMid\n" +
                    "         WHERE u.mid NOT IN (SELECT followeeMid FROM UserFollowings)\n" +
                    "         GROUP BY u.mid, u.level\n" +
                    "     )\n" +
                    "SELECT mid, level\n" +
                    "FROM RecommendedUsers\n" +
                    "ORDER BY common_followings DESC, level DESC, mid ASC\n"+
                    "LIMIT ? OFFSET ?;")){
                ps.setLong(1,userRecord.getMid());
                ps.setLong(2,userRecord.getMid());

                ps.setInt(3, pageSize);
                ps.setInt(4, (pageNum - 1) * pageSize);
                try(ResultSet rs = ps.executeQuery();) {
                    while (rs.next()) {
                        recommend.add(rs.getLong("mid"));
                    }
                    return recommend;
                }
            }

        }catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
