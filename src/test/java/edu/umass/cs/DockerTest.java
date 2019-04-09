package edu.umass.cs;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import org.junit.Test;

import java.util.List;

public class DockerTest {
    @Test
    public void name() {
        DockerClient client = DockerClientBuilder.getInstance().build();

        Ports ports = new Ports();
        ExposedPort tcp = ExposedPort.tcp(80);
        ports.bind(tcp, Ports.Binding.empty());
        CreateContainerResponse nginx = client
                .createContainerCmd("nginx")
                .withHostConfig(HostConfig.newHostConfig().withPortBindings(ports))
                .exec();
        client.startContainerCmd(nginx.getId()).exec();

        InspectContainerResponse inspect = client.inspectContainerCmd(nginx.getId()).exec();
        Ports.Binding binding = inspect.getNetworkSettings().getPorts().getBindings().get(tcp)[0];
        String hostPortSpec = binding.getHostPortSpec();
        System.out.println(Integer.parseInt(hostPortSpec));
    }
}
