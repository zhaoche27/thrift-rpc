package com.ttyc.datagroup.commons.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class Nets {

    private static final Logger logger = LoggerFactory.getLogger(Nets.class);

    public static final String LOCALHOST = "127.0.0.1";

    public static final String ANYHOST = "0.0.0.0";

    private static final int RND_PORT_START = 30000;

    private static final int RND_PORT_RANGE = 10000;

    private static final int MAX_PORT = 65535;

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private static final Pattern IP_PATTERN = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3,5}$");

    private static String localIp;

    public static String getLocalIp() {
        if (localIp == null) {
            localIp = localIp();
        }
        return localIp;
    }

    private static boolean isValidAddress(InetAddress address) {
        if (address == null || address.isLoopbackAddress())
            return false;
        String name = address.getHostAddress();
        return (name != null
                && !ANYHOST.equals(name)
                && !LOCALHOST.equals(name)
                && IP_PATTERN.matcher(name).matches());
    }

    /**
     * 获取本机IP地址
     *
     * @return
     */
    private static String localIp() {
        // 一个主机有多个网络接口
        String localIp = null;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    try {
                        NetworkInterface network = interfaces.nextElement();
                        Enumeration<InetAddress> addresses = network.getInetAddresses();
                        if (addresses != null) {
                            while (addresses.hasMoreElements()) {
                                try {
                                    InetAddress address = addresses.nextElement();
                                    if (isValidAddress(address)) {
                                        localIp = address.getHostAddress();
                                    }
                                } catch (Throwable e) {
                                    logger.warn("Failed to retriving ip address, " + e.getMessage(), e);
                                }
                            }
                        }
                    } catch (Throwable e) {
                        logger.warn("Failed to retriving ip address, " + e.getMessage(), e);
                    }
                }
            }
            if (!validateLocalIp(localIp)) {
                throw new IllegalArgumentException("Failed to retriving ip address:" + localIp);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return localIp;
    }

    /**
     * 验证本地IP
     *
     * @param localIp
     * @Return
     */
    private static boolean validateLocalIp(String localIp) {
        if (localIp == null || localIp.isEmpty() || localIp.equals(LOCALHOST)) {
            return false;
        }
        return true;
    }

    public static int getRandomPort() {
        return RND_PORT_START + RANDOM.nextInt(RND_PORT_RANGE);
    }

    /**
     * 自动获取可用TCP端口
     *
     * @return
     */
    public static int getAvailablePort() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket();
            ss.bind(null);
            return ss.getLocalPort();
        } catch (IOException e) {
            return getRandomPort();
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * 自动获取可用TCP端口
     *
     * @param port
     * @return
     */
    public static int getAvailablePort(int port) {
        if (port <= 0) {
            return getAvailablePort();
        }
        for (int i = port; i < MAX_PORT; i++) {
            ServerSocket ss = null;
            try {
                ss = new ServerSocket(i);
                return i;
            } catch (IOException e) {
                // continue
            } finally {
                if (ss != null) {
                    try {
                        ss.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
        return port;
    }

}
