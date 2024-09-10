package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DanmuService;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.sustc.service.impl.Utils;
import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.sustc.service.impl.Utils;

import static io.sustc.service.impl.Utils.*;
import static io.sustc.service.impl.Utils.isDone;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    public DataSource dataSource;

    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if(auth==null)return -1;
        if(content==null||content.isEmpty())return -1;
        try (Connection connection = dataSource.getConnection()) {
            UserRecord userRecord=Utils.isValidAuth(connection,auth);
            if(userRecord==null)return -1;
            if (!Utils.isPublic(connection,bv,time))return -1;
            try(PreparedStatement ps2 = connection.prepareStatement(
                    "SELECT 1 FROM ViewRela WHERE viewerMid = ? AND bv = ?")){
                ps2.setLong(1,userRecord.getMid());
                ps2.setString(2,bv);
                if(!ps2.executeQuery().next())return -1;
            }
            try(PreparedStatement ps4 = connection.prepareStatement("UPDATE Videos SET danmuCount=danmuCount+1 where bv = ?")){
                ps4.setString(1,bv);
                ps4.execute();
            }
            try(PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO Danmu (bv, mid, content, time, postTime) VALUES (?, ?, ?, ?, ?)",
                    PreparedStatement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, bv);
                ps.setLong(2, userRecord.getMid());
                ps.setString(3, content);
                ps.setFloat(4, time);
                ps.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
                ps.executeUpdate();
                try(ResultSet rs=ps.getGeneratedKeys()){
                    rs.next();
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        if (timeStart < 0 || timeEnd < 0 || timeStart > timeEnd) {
            return null;
        }
        String sql = filter
                ? "SELECT DISTINCT ON (content) id \n" +
                "FROM (SELECT id, content, time FROM Danmu\n" +
                "    WHERE bv = ?\n" +
                "    ORDER BY postTime) AS subquery\n" +
                "WHERE time >= ? AND time <= ?\n" +
                "ORDER BY content, time;"
                : "SELECT id FROM Danmu WHERE bv = ? AND time >= ? AND time <= ? ORDER BY time";

        try (Connection connection = dataSource.getConnection();
             ) {
            if(!Utils.isPublic(connection,bv,timeEnd))return null;
            try(PreparedStatement ps = connection.prepareStatement(sql)){
                ps.setString(1, bv);
                ps.setFloat(2, timeStart);
                ps.setFloat(3, timeEnd);
                try (ResultSet rs = ps.executeQuery()) {
                    List<Long> danmuIds = new ArrayList<>();
                    while (rs.next()) {
                        danmuIds.add(rs.getLong("id"));
                    }
                    return danmuIds;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        String bv=null;
        try(Connection connection= dataSource.getConnection();){
            UserRecord userRecord=isValidAuth(connection,auth);
            if(userRecord==null)return false;
            try(PreparedStatement ps2 = connection.prepareStatement(
                    "SELECT bv FROM Danmu WHERE id = ?;")){
                ps2.setLong(1,id);
                ResultSet rs2 = ps2.executeQuery();
                if(!rs2.next()){rs2.close();return false;}
                bv = rs2.getString(1);
                rs2.close();
            }
            try(PreparedStatement ps2 = connection.prepareStatement(
                    "SELECT 1 FROM ViewRela WHERE viewerMid = ? AND bv = ?")){
                ps2.setLong(1,userRecord.getMid());
                ps2.setString(2,bv);
                if(!ps2.executeQuery().next())return false;
            }

            Boolean isDone=false;
            try(PreparedStatement ps2 = connection.prepareStatement(
                    "SELECT 1 FROM DanmuRela WHERE mid = ? AND id = ?")){
                ps2.setLong(1,userRecord.getMid());
                ps2.setLong(2,id);
                if(ps2.executeQuery().next())isDone=true;
            }

                if(isDone){
                    try (PreparedStatement ps = connection.prepareStatement("DELETE FROM DanmuRela WHERE mid = ? AND id = ?;");
                    ) {ps.setLong(1,userRecord.getMid());
                        ps.setLong(2,id);
                        if (ps.executeUpdate() == 1) return true;
                        return false;
                    }
                }else {
                    try (PreparedStatement ps = connection.prepareStatement("INSERT INTO DanmuRela (mid,id) VALUES (?,?);");
                    ) {
                        ps.setLong(1, userRecord.getMid());
                        ps.setLong(2, id);
                        if (ps.executeUpdate() == 1) return true;
                        return false;
                    }
                }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
