using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Util;
using FluentAssertions;
using Xunit;
using Zyng.Akka.Cluster.SplitBrain.Resolver;
using AC = Akka.Cluster;

namespace Zynga.Akka.Cluster.Resolvers.Tests
{
  public class TestDowningProvider : RoleBasedProvider
  {
    public readonly AtomicBoolean ActorPropsAccessed = new AtomicBoolean(false);

    public override Props DowningActorProps
    {
      get
      {
        ActorPropsAccessed.Value = true;
        return base.DowningActorProps;
      }
    }

    public TestDowningProvider(ActorSystem system) : base(system)
    {
    }
  }

  public class DowningProviderSpec : global::Akka.TestKit.Xunit2.TestKit
  {
    public readonly Config BaseConfig = ConfigurationFactory.ParseString(@"
          akka {
            loglevel = WARNING
            actor.provider = ""Akka.Cluster.ClusterActorRefProvider, Akka.Cluster""
            remote {
              dot-netty.tcp {
                hostname = ""127.0.0.1""
                port = 0
              }
            }
          }
        ").WithFallback(ConfigurationFactory.Load());

    [Fact]
    public void Downing_provider_should_use_specified_downing_provider()
    {
      var config = ConfigurationFactory.ParseString(
        @"akka.cluster.downing-provider-class = ""Zynga.Akka.Cluster.Resolvers.Tests.TestDowningProvider, Zynga.Akka.Cluster.Tests""");
      using (var system = ActorSystem.Create("auto-downing", config.WithFallback(BaseConfig)))
      {
        var downingProvider = AC.Cluster.Get(system).DowningProvider;
        downingProvider.Should().BeAssignableTo<RoleBasedProvider>();
        AwaitCondition(() =>
            (downingProvider as TestDowningProvider).ActorPropsAccessed.Value,
          TimeSpan.FromSeconds(3));
      }
    }
  }
}