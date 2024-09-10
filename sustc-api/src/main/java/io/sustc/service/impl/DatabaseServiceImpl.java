package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    public DataSource dataSource;

    private static final int THREAD_POOL_SIZE = 7;
    public static final String key = "90ac329cb8871adb3ed2";

    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12211829);
    }
    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {try{

        //createTablesIfNeeded(connection);
        importUsersExecutorService(userRecords);
        importDanmuExecutorService(danmuRecords);
        importVideosExecutorService(videoRecords);
            /*
            String buildAutoMidGenerater = "CREATE OR REPLACE FUNCTION generate_unique_mid()\n" +
                    "    RETURNS BIGINT AS $$\n" +
                    "DECLARE\n" +
                    "    new_mid BIGINT;\n" +
                    "BEGIN\n" +
                    "    LOOP\n" +
                    "        -- 生成随机的长整型值\n" +
                    "        new_mid := (RANDOM() * 9223372036854775807)::BIGINT;\n" +
                    "\n" +
                    "        -- 检查这个值是否已经存在\n" +
                    "        IF NOT EXISTS (SELECT 1 FROM Users WHERE mid = new_mid) THEN\n" +
                    "            RETURN new_mid;\n" +
                    "        END IF;\n" +
                    "    END LOOP;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql;\n" +
                    "\n" +
                    "CREATE OR REPLACE FUNCTION set_mid()\n" +
                    "    RETURNS TRIGGER AS $$\n" +
                    "BEGIN\n" +
                    "    IF NEW.mid IS NULL THEN\n" +
                    "        NEW.mid := generate_unique_mid();\n" +
                    "    END IF;\n" +
                    "    RETURN NEW;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql;\n" +
                    "\n" +
                    "DO $$\n" +
                    "BEGIN\n" +
                    "    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_set_mid') THEN\n" +
                    "        CREATE TRIGGER trigger_set_mid\n" +
                    "            BEFORE INSERT ON Users\n" +
                    "            FOR EACH ROW\n" +
                    "        EXECUTE FUNCTION set_mid();\n" +
                    "    END IF;\n" +
                    "END\n" +
                    "$$;\n";
            String buildAutoBvGenerater = "CREATE OR REPLACE FUNCTION generate_unique_bv()\n" +
                    "    RETURNS VARCHAR AS $$\n" +
                    "DECLARE\n" +
                    "    new_bv VARCHAR(13);\n" +
                    "    i INT;\n" +
                    "    ch CHAR;\n" +
                    "BEGIN\n" +
                    "    LOOP\n" +
                    "        new_bv := 'BV';\n" +
                    "\n" +
                    "        FOR i IN 1..10 LOOP\n" +
                    "                ch := SUBSTRING(MD5(RANDOM()::TEXT) FROM i FOR 1);\n" +
                    "                -- 是否将字符转换为大写\n" +
                    "                IF RANDOM() < 0.5 THEN\n" +
                    "                    new_bv := new_bv || UPPER(ch);\n" +
                    "                ELSE\n" +
                    "                    new_bv := new_bv || ch;\n" +
                    "                END IF;\n" +
                    "            END LOOP;\n" +
                    "\n" +
                    "        -- 检查是否已存在\n" +
                    "        IF NOT EXISTS (SELECT 1 FROM Videos WHERE bv = new_bv) THEN\n" +
                    "            RETURN new_bv;\n" +
                    "        END IF;\n" +
                    "    END LOOP;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql;\n" +
                    "\n" +
                    "CREATE OR REPLACE FUNCTION set_bv()\n" +
                    "    RETURNS TRIGGER AS $$\n" +
                    "BEGIN\n" +
                    "    IF NEW.bv IS NULL THEN\n" +
                    "        NEW.bv := generate_unique_bv();\n" +
                    "    END IF;\n" +
                    "    RETURN NEW;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql;\n" +
                    "\n" +
                    "DO $$\n" +
                    "    BEGIN\n" +
                    "        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_set_bv') THEN\n" +
                    "            CREATE TRIGGER trigger_set_bv\n" +
                    "                BEFORE INSERT ON Videos\n" +
                    "                FOR EACH ROW\n" +
                    "            EXECUTE FUNCTION set_bv();\n" +
                    "        END IF;\n" +
                    "    END\n" +
                    "$$;";
            String buildIndex = "CREATE INDEX IF NOT EXISTS idx_FollRela_followerMid\n" +
                    "    ON FollRela (followerMid);\n" +
                    "CREATE INDEX IF NOT EXISTS idx_FollRela_followeeMid\n" +
                    "    ON FollRela (followeeMid);\n" +
                    "CREATE INDEX IF NOT EXISTS idx_ViewRela_viewerMid\n" +
                    "    ON ViewRela (viewerMid);\n" +
                    "CREATE INDEX IF NOT EXISTS idx_ViewRela_bv\n" +
                    "    ON ViewRela (bv);\n" +
                    "CREATE INDEX IF NOT EXISTS idx_InteractionRela_bv\n" +
                    "    ON InteractionRela (bv);\n" +
                    "CREATE INDEX IF NOT EXISTS idx_InteractionRela_comp\n" +
                    "    ON InteractionRela (mid,bv,behavior);"+
                    "CREATE INDEX IF NOT EXISTS idx_Videos_ownerMid\n" +
                    "    ON Videos (ownerMid);"+
                    "CREATE INDEX IF NOT EXISTS idx_Users_qq\n" +
                    "    ON Users (qq);\n" +
                    "CREATE INDEX IF NOT EXISTS idx_Users_wechat\n" +
                    "    ON Users (wechat);\n" +
                    "CREATE INDEX IF NOT EXISTS idx_Danmu_time\n" +
                    "    ON Danmu (time);\n" +
                    "CREATE INDEX IF NOT EXISTS idx_DanmuRela_mid\n" +
                    "    ON DanmuRela (mid);";
            String buildStringMatchFunction="CREATE OR REPLACE FUNCTION count_non_overlapping_substrings(main_str text, sub_str text) RETURNS integer AS $$\n" +
                    "DECLARE\n" +
                    "    count integer := 0;\n" +
                    "    sub_str_array text[];\n" +
                    "    current_sub_str text;\n" +
                    "BEGIN\n" +
                    "    IF sub_str = '' OR main_str = '' THEN\n" +
                    "        RETURN 0;\n" +
                    "    END IF;\n" +
                    "    sub_str_array := regexp_split_to_array(sub_str, '\\s+');\n" +
                    "\n" +
                    "    FOREACH current_sub_str IN ARRAY sub_str_array\n" +
                    "        LOOP\n" +
                    "            count := count + regexp_count(main_str, '(?i)' || regexp_replace(current_sub_str, '([.^$*+?{}|()])', '\\\\\\1', 'g'));\n" +
                    "        END LOOP;\n" +
                    "\n" +
                    "    RETURN count;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql;";

            connection.prepareStatement(buildAutoMidGenerater).execute();
            connection.prepareStatement(buildStringMatchFunction).execute();
            connection.prepareStatement(buildIndex).execute();
            connection.prepareStatement(buildAutoBvGenerater).execute();
            */
    }catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
    }
    private void createTablesIfNeeded(Connection connection) throws SQLException {
        // Create User table
        String createUserTable = "CREATE TABLE IF NOT EXISTS Users (\n" +
                "    mid BIGINT PRIMARY KEY,\n" +
                "    name VARCHAR(50),\n" +
                "    sex VARCHAR(10),\n" +
                "    birthday VARCHAR(20),\n" +
                "    level SMALLINT,\n" +
                "    sign VARCHAR(100),\n" +
                "    coin INT,\n" +
                "    identity VARCHAR(10),\n" +
                "    password VARCHAR(32),\n" +
                "    qq VARCHAR(50),\n" +
                "    wechat VARCHAR(50)\n" +
                ");";
        connection.prepareStatement(createUserTable).execute();

        // Create Video table
        String createVideoTable = "CREATE TABLE IF NOT EXISTS Videos (\n" +
                "    bv VARCHAR(255) PRIMARY KEY,\n" +
                "    title VARCHAR(50),\n" +
                "    ownerMid BIGINT,\n" +
                "    ownerName VARCHAR(20),\n" +
                "    commitTime TIMESTAMP,\n" +
                "    reviewTime TIMESTAMP,\n" +
                "    publicTime TIMESTAMP,\n" +
                "    duration FLOAT,\n" +
                "    description TEXT,\n" +
                "    reviewer BIGINT,\n" +
                "    viewCount BIGINT,\n" +
                "    coinCount BIGINT,\n" +
                "    likeCount BIGINT,\n" +
                "    collectCount BIGINT,\n" +
                "    danmuCount BIGINT,\n" +
                "    watchTime DOUBLE PRECISION\n" +
                ");";

        connection.prepareStatement(createVideoTable).execute();

        // Create Danmu table
        String createDanmuTable = "CREATE TABLE IF NOT EXISTS Danmu (\n" +
                "    id BIGSERIAL PRIMARY KEY,\n" +
                "    bv VARCHAR(255),\n" +
                "    mid BIGINT,\n" +
                "    \"time\" FLOAT,\n" +
                "    content TEXT,\n" +
                "    postTime TIMESTAMP\n" +
                ");";

        connection.prepareStatement(createDanmuTable).execute();

        String createDanmuRela = "CREATE TABLE IF NOT EXISTS DanmuRela (\n" +
                "    mid BIGINT,\n" +
                "    id BIGINT,\n" +
                "    PRIMARY KEY (mid,id)\n" +
                ");";

        connection.prepareStatement(createDanmuRela).execute();


        String createFollRelaTable = "CREATE TABLE IF NOT EXISTS FollRela (\n" +
                "    followerMid BIGINT,\n" +
                "    followeeMid BIGINT,\n" +
                "    PRIMARY KEY (followerMid,followeeMid)\n" +
                ");";

        connection.prepareStatement(createFollRelaTable).execute();

        String createViewRela = "CREATE TABLE IF NOT EXISTS ViewRela (\n" +
                "    viewerMid BIGINT,\n" +
                "    bv VARCHAR(255),\n" +
                "    viewTime FLOAT\n" +
                ");";

        connection.prepareStatement(createViewRela).execute();

        String createInteractionRela = "CREATE TABLE IF NOT EXISTS InteractionRela (\n" +
                "    mid BIGINT,\n" +
                "    bv VARCHAR(255),\n" +
                "    behavior VARCHAR(20) CHECK (behavior IN ('like', 'coin', 'collect'))\n" +
                ");";

        connection.prepareStatement(createInteractionRela).execute();

    }

    private void importUsers(Connection connection, List<UserRecord> userRecords) throws SQLException {
        String insertUser = "INSERT INTO Users (mid,name,sex,birthday,level,sign,coin,identity,password,qq,wechat) VALUES (?,?,?,?,?,?,?,?,?,?,?);"; // Replace with actual attributes
        String insertFollRela = "INSERT INTO FollRela (followerMid,followeeMid) VALUES (?,?);";
        long count=0;
        try(PreparedStatement ps = connection.prepareStatement(insertUser);
            PreparedStatement ps2 = connection.prepareStatement(insertFollRela)) {
            for (UserRecord user : userRecords) {
                ps.setLong(1, user.getMid());
                ps.setString(2, user.getName());
                ps.setString(3, user.getSex());
                ps.setString(4, user.getBirthday());
                ps.setShort(5, user.getLevel());
                ps.setString(6, user.getSign());
                ps.setInt(7, user.getCoin());
                if(user.getIdentity()==null)user.setIdentity(UserRecord.Identity.USER);
                ps.setString(8, String.valueOf(user.getIdentity()));
                ps.setString(9, Utils.encrypt(user.getPassword(),key));
                ps.setString(10, user.getQq());
                ps.setString(11, user.getWechat());
                ps.addBatch();
                ++count;
                if (user.getFollowing() != null) {
                    for (long followeemid : user.getFollowing()) {
                        ps2.setLong(1, user.getMid());
                        ps2.setLong(2, followeemid);
                        ps2.addBatch();
                    }
                }
                if(count%100==0){ps.executeBatch();ps2.executeBatch();}
            }
            ps.executeBatch();
            ps2.executeBatch();
            ps.close();
            ps2.close();
        }
    }

    private void importVideos(Connection connection, List<VideoRecord> videoRecords) throws SQLException {
        String insertVideos = "INSERT INTO Videos (bv,title,ownerMid,ownerName,commitTime,reviewTime,publicTime,duration,description,reviewer,viewCount,coinCount,likeCount,collectCount,watchTime,danmuCount) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"; // Replace with actual attributes
        String insertViewRela = "INSERT INTO ViewRela (viewerMid,bv,viewTime) VALUES (?,?,?);";
        String insertInteractionRela = "INSERT INTO InteractionRela (mid,bv,behavior) VALUES (?,?,?);";
        long count=0;
        try(PreparedStatement ps = connection.prepareStatement(insertVideos);
            PreparedStatement ps2 = connection.prepareStatement(insertViewRela);
            PreparedStatement ps3 = connection.prepareStatement(insertInteractionRela)) {
            for (VideoRecord video : videoRecords) {
                ps.setString(1, video.getBv());
                ps.setString(2, video.getTitle());
                ps.setLong(3, video.getOwnerMid());
                ps.setString(4, video.getOwnerName());
                ps.setTimestamp(5, video.getCommitTime());
                ps.setTimestamp(6, video.getReviewTime());
                ps.setTimestamp(7, video.getPublicTime());
                ps.setFloat(8, video.getDuration());
                ps.setString(9, video.getDescription());
                ps.setLong(10, video.getReviewer());
                if (video.getViewerMids() == null) {
                    ps.setLong(11, 0);
                    ps.setDouble(15,0);
                } else {ps.setLong(11, video.getViewerMids().length);
                    double sum = 0.0;
                    for (float value : video.getViewTime()) {
                        sum += value;
                    }
                    ps.setDouble(15,sum);
                }

                if (video.getCoin()==null){
                    ps.setLong(12,0);
                }else ps.setLong(12,video.getCoin().length);

                if (video.getLike()==null){
                    ps.setLong(13,0);
                }else ps.setLong(13,video.getLike().length);

                if(video.getFavorite()==null){
                    ps.setLong(14,0);
                }else ps.setLong(14,video.getFavorite().length);
                try(PreparedStatement ps4 = connection.prepareStatement("SELECT COUNT(*) FROM danmu WHERE bv = ? ;")){
                    ps4.setString(1,video.getBv());
                    ResultSet rs4 = ps4.executeQuery();
                    rs4.next();
                    ps.setLong(16,rs4.getLong(1));
                }
                ps.addBatch();
                ++count;
                long[] viewerMid = video.getViewerMids();
                float[] viewTime = video.getViewTime();
                if (viewerMid != null) {
                    int n = viewerMid.length;
                    for (int i = 0; i < n; ++i) {
                        ps2.setLong(1, viewerMid[i]);
                        ps2.setFloat(3, viewTime[i]);
                        ps2.setString(2, video.getBv());
                        ps2.addBatch();
                    }

                }
                long[] Mid = video.getLike();
                if (Mid != null) {
                    for (long likeMid : Mid) {
                        ps3.setLong(1, likeMid);
                        ps3.setString(2, video.getBv());
                        ps3.setString(3, "like");
                        ps3.addBatch();
                    }
                }
                Mid = video.getCoin();
                if (Mid != null) {
                    for (long coinMid : Mid) {
                        ps3.setLong(1, coinMid);
                        ps3.setString(2, video.getBv());
                        ps3.setString(3, "coin");
                        ps3.addBatch();
                    }
                }
                Mid = video.getFavorite();
                if (Mid != null) {
                    for (long favoriteMid : Mid) {
                        ps3.setLong(1, favoriteMid);
                        ps3.setString(2, video.getBv());
                        ps3.setString(3, "collect");
                        ps3.addBatch();
                    }
                }
                if(count%50==0){
                    ps.executeBatch();
                    ps2.executeBatch();
                    ps3.executeBatch();
                }
            }
            ps.executeBatch();
            ps2.executeBatch();
            ps3.executeBatch();
            ps.close();
            ps2.close();
            ps3.close();
        }
    }
    public void importDanmuExecutorService(List<DanmuRecord> danmuRecords) throws InterruptedException {
        int chunkSize = danmuRecords.size() / THREAD_POOL_SIZE;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            int start = i * chunkSize;
            int end = (i == THREAD_POOL_SIZE - 1) ? danmuRecords.size() : (start + chunkSize);

            Runnable task = () -> {
                try (Connection connection = dataSource.getConnection()) {
                    importDanmu(connection, danmuRecords.subList(start, end));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };
            executor.submit(task);
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
    public void importUsersExecutorService(List<UserRecord> userRecords) throws InterruptedException {
        int chunkSize = userRecords.size() / THREAD_POOL_SIZE;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            int start = i * chunkSize;
            int end = (i == THREAD_POOL_SIZE - 1) ? userRecords.size() : (start + chunkSize);

            Runnable task = () -> {
                try (Connection connection = dataSource.getConnection()) {
                    importUsers(connection, userRecords.subList(start, end));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };
            executor.submit(task);
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    public void importVideosExecutorService(List<VideoRecord> videoRecords) throws InterruptedException {
        int chunkSize = videoRecords.size() / THREAD_POOL_SIZE;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            int start = i * chunkSize;
            int end = (i == THREAD_POOL_SIZE - 1) ? videoRecords.size() : (start + chunkSize);

            Runnable task = () -> {
                try (Connection connection = dataSource.getConnection()) {
                    importVideos(connection, videoRecords.subList(start, end));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            };
            executor.submit(task);
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }





    private void importDanmu(Connection connection, List<DanmuRecord> danmuRecords) throws SQLException {
        String insertDanmu = "INSERT INTO Danmu (bv,mid,time,content,postTime) VALUES (?,?,?,?,?);";
        String insertDanmulike = "INSERT INTO DanmuRela (mid,id) VALUES (?,?);";
        try (PreparedStatement ps = connection.prepareStatement(insertDanmu, Statement.RETURN_GENERATED_KEYS);
             PreparedStatement ps2 = connection.prepareStatement(insertDanmulike)) {
            long count=0;
            // 批量插入Danmu记录
            for (DanmuRecord danmu : danmuRecords) {
                ps.setString(1, danmu.getBv());
                ps.setLong(2, danmu.getMid());
                ps.setFloat(3, danmu.getTime());
                ps.setString(4, danmu.getContent());
                ps.setTimestamp(5, danmu.getPostTime());
                ps.addBatch();
                ++count;
                if(count%10000==0)ps.executeBatch();
            }
            ps.executeBatch();

            // 获取所有插入记录的ID
            List<Long> generatedIds = new ArrayList<>();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                while (rs.next()) {
                    generatedIds.add(rs.getLong(1));
                }
            }

            // 检查是否有足够的ID
            if (generatedIds.size() != danmuRecords.size()) {
                throw new SQLException("生成的ID数量与插入记录数量不符");
            }

            // 批量插入DanmuRela记录
            long count1=0;
            for (int i = 0; i < danmuRecords.size(); i++) {
                DanmuRecord danmu = danmuRecords.get(i);
                long id = generatedIds.get(i);
                if (danmu.getLikedBy() != null) {
                    for (long likerecord : danmu.getLikedBy()) {
                        ps2.setLong(1, likerecord);
                        ps2.setLong(2, id);
                        ps2.addBatch();
                        ++count1;
                    }
                }
                if(count1>10000){
                    count1=0;
                    ps2.executeBatch();
                }
            }
            ps2.executeBatch();
        }
    }
    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
