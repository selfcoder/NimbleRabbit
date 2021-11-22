using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Docker.DotNet;
using Docker.DotNet.Models;
using Xunit;

namespace NimbleRabbit.Tests
{
    public class RabbitMQFixture : IAsyncLifetime
    {
        private const string ImageName = "rabbitmq";
        
        private readonly DockerClient _client;
        private string _containerId;

        public RabbitMQFixture()
        {
            string uri = OperatingSystem.IsWindows()
                ? "npipe://./pipe/docker_engine"
                : "unix:///var/run/docker.sock";
            _client = new DockerClientConfiguration(new Uri(uri))
                .CreateClient();
        }
        
        public async Task InitializeAsync()
        {
            var response = await _client.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = ImageName,
                Name = "TestName",
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        { "5672/tcp", new List<PortBinding> { new() { HostPort = "5672" } } }
                    }
                }
            });
            _containerId = response.ID;
            await _client.Containers.StartContainerAsync(
                _containerId,
                new ContainerStartParameters()
            );
        }

        public async Task DisposeAsync()
        {
            await _client.Containers.StopContainerAsync(_containerId, new ContainerStopParameters());
            await _client.Containers.RemoveContainerAsync(_containerId, new ContainerRemoveParameters());
        }
    }
}