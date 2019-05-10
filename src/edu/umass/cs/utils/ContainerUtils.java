package edu.umass.cs.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ContainerUtils {

    public static String createContainer(String name, String image) {
        try {
            final Process p = Runtime.getRuntime().exec(String.format(
                    "docker create -P --name %s --security-opt seccomp:unconfined %s", name, image
            ));
            final String containerId = getResult(p);
            p.waitFor();
            return containerId;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void startContainer(String name, String checkpoint) {
        try {
            String cmd = "docker start %s";
            if (checkpoint != null) {
                cmd = "docker start --checkpoint " + checkpoint + " %s";
            }
            Runtime.getRuntime().exec(String.format(cmd, name)).waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void stopContainer(String name) {
        try {
            Runtime.getRuntime().exec(String.format("docker rm -f %s", name)).waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void checkpoint(String name, String checkpointName) {
        try {
            Runtime.getRuntime().exec(String.format("docker checkpoint create %s %s", name, checkpointName)).waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String getRunningContainerId(String name) {
        try {
            final Process p = Runtime.getRuntime().exec("docker ps -q --no-trunc --filter name=" + name);
            final String result = getResult(p).trim();
            p.waitFor();
            return result.isEmpty() ? null : result;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Integer getPort(String name) {
        try {
            final Process p = Runtime.getRuntime().exec(String.format("docker port %s", name));
            final String port = getResult(p).split(":")[1];
            p.waitFor();
            return Integer.valueOf(port);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getResult(Process process) {
        final StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
}
