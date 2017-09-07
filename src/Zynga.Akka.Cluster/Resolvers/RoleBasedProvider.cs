using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.Cluster;

namespace Zyng.Akka.Cluster.SplitBrain.Resolver
{
  public class RoleBasedProvider : IDowningProvider
  {
    private ActorSystem _system;
    private readonly List<string> _essentialRoles;

    public RoleBasedProvider(ActorSystem system)
    {
      _system = system;

      var config = _system.Settings.Config;

      DownRemovalMargin = config.GetTimeSpan("app.cluster.stable-after");
      _essentialRoles = config.GetStringList("app.cluster.essential-roles").ToList();
    }

    public TimeSpan DownRemovalMargin { get; }

    public virtual Props DowningActorProps => RoleBasedResolver.props(DownRemovalMargin, _essentialRoles);
  }
}