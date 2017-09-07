using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using AC = Akka.Cluster;
using Akka.Event;
using Akka.Remote;

namespace Zyng.Akka.Cluster.SplitBrain.Resolver
{
  public class DownAction
  {
  }

  public class DownReachable : DownAction
  {
  }

  public class DownUnreachable : DownAction
  {
  }

  public class DownAll : DownAction
  {
  }

  public class Tick
  {
  }

  public class ByMemberAddress : IComparer<AC.Member>
  {
    public int Compare(AC.Member x, AC.Member y)
    {
      return x.UniqueAddress.CompareTo(y.UniqueAddress);
    }
  }

  public class RoleBasedResolver : UntypedActor
  {
    public static Props props(TimeSpan stableAfter, List<string> essentialRoles)
    {
      return new Props(typeof(RoleBasedResolver), new object[] {stableAfter, essentialRoles});
    }

    private readonly TimeSpan _stableAfter;
    private readonly List<string> _essentialRoles;
    private readonly AC.Cluster _cluster = AC.Cluster.Get(Context.System);

    private readonly List<AC.MemberStatus> _ignoreMemberStatus =
      new List<AC.MemberStatus> {AC.MemberStatus.Down, AC.MemberStatus.Exiting};

    private readonly List<AC.UniqueAddress> _unreachable = new List<AC.UniqueAddress>();
    private readonly ICancelable _tickTask;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    private bool _isLeader;
    private bool _isSelfAdded;

    private ImmutableSortedSet<AC.Member> _members =
      ImmutableSortedSet<AC.Member>.Empty.WithComparer(new ByMemberAddress());

    private Deadline _stableDeadline;

    private AC.UniqueAddress SelfUniqueAddress => _cluster.SelfUniqueAddress;

    public RoleBasedResolver(TimeSpan stableAfter, List<string> essentialRoles)
    {
      _stableAfter = stableAfter;
      _essentialRoles = essentialRoles;

      var interval = TimeSpan.FromMilliseconds(Math.Max(stableAfter.Milliseconds / 2,
        TimeSpan.FromMilliseconds(500).Milliseconds));

      _tickTask = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(interval, interval, Self, new Tick(),
        ActorRefs.Nobody);
    }

    private Deadline resetStableDeadline()
    {
      return Deadline.Now + _stableAfter;
    }

    private IEnumerable<AC.UniqueAddress> nodeAddress()
    {
      return _members.Select(x => x.UniqueAddress).ToList();
    }

    private IEnumerable<AC.UniqueAddress> reachableNodesAddress => _members
      .Where(x => !_unreachable.Contains(x.UniqueAddress)).Select(x => x.UniqueAddress).ToList();

    private IEnumerable<AC.Member> unreachableMembers => _members.Where(x => _unreachable.Contains(x.UniqueAddress));
    private IEnumerable<AC.Member> reachableMembers => _members.Where(x => !_unreachable.Contains(x.UniqueAddress));

    public IEnumerable<AC.UniqueAddress> joining => _cluster.State.Members
      .Where(x => x.Status == AC.MemberStatus.Joining)
      .Select(y => y.UniqueAddress);

    protected override void PreStart()
    {
      _cluster.Subscribe(Self, AC.ClusterEvent.SubscriptionInitialStateMode.InitialStateAsEvents,
        typeof(AC.ClusterEvent.IClusterDomainEvent));
      base.PreStart();
    }

    protected override void PostStop()
    {
      _cluster.Unsubscribe(Self);
      _tickTask.Cancel();

      base.PostStop();
    }

    private void down(Address node)
    {
      if (!_isLeader)
      {
        return;
      }

      _cluster.Down(node);
    }

    public DownAction decide()
    {
      var unreachableOK = unreachableMembers.SelectMany(x => x.Roles).Except(_essentialRoles).Any();
      var reachableOK = reachableMembers.SelectMany(x => x.Roles).Except(_essentialRoles).Any();

      _log.Info("unreachableOK: {0}, reachableOK: {1}", unreachableOK, reachableOK);

      if (unreachableOK && reachableOK)
      {
        var unreachableSize = unreachableMembers.Count();
        var membersSize = _members.Count;

        _log.Info("unreachableSize: {0}, membersSize: {1}", unreachableSize, membersSize);
        if (unreachableSize * 2 == membersSize)
        {
          _log.Info(
            "Both partitions are equal in size, break the tie by keeping the side with oldest member");

          if (_unreachable.Contains(_members.First().UniqueAddress))
          {
            return new DownReachable();
          }
          return new DownUnreachable();
        }
        if (unreachableSize * 2 < membersSize)
        {
          _log.Info("We are in majority, DownUnreachable");
          return new DownUnreachable();
        }
        _log.Info("We are in minority, DownReachable");
        return new DownReachable();
      }
      if (unreachableOK)
      {
        _log.Info("Only unreachable nodes have essential roles, DownReachable");
        return new DownReachable();
      }
      if (reachableOK)
      {
        _log.Info("Only we have essential roles, DownUnreachable");
        return new DownUnreachable();
      }
      _log.Info("No side has essential roles, DownAll");
      return new DownAll();
    }

    protected override void OnReceive(object message)
    {
      switch (message)
      {
        case AC.ClusterEvent.UnreachableMember m:
          unreachableMember(m.Member);
          break;
        case AC.ClusterEvent.ReachableMember m:
          reachableMember(m.Member);
          break;
        case AC.ClusterEvent.MemberUp m:
          memberUp(m.Member);
          break;
        case AC.ClusterEvent.MemberRemoved m:
          memberRemoved(m.Member);
          break;
        case AC.ClusterEvent.LeaderChanged m:
          _isLeader = m.Leader == SelfUniqueAddress.Address;
          break;
        case Tick m:
        {
          var shouldAct = _isLeader && _isSelfAdded && _unreachable.Any() && _stableDeadline.IsOverdue;

          if (shouldAct)
          {
            var downAction = decide();
            var nodesToDown = getDownedNodes(downAction);

            if (nodesToDown.Count != 0)
            {
              var downSelf = nodesToDown.Contains(SelfUniqueAddress);
              foreach (var item in nodesToDown)
              {
                if (item != SelfUniqueAddress)
                {
                  down(item.Address);
                }
              }

              if (downSelf)
              {
                down(SelfUniqueAddress.Address);
              }

              _stableDeadline = resetStableDeadline();
            }
          }
        }
          break;
      }
    }

    public List<AC.UniqueAddress> getDownedNodes(DownAction action)
    {
      switch (action)
      {
        case DownUnreachable _: return _unreachable;
        case DownReachable _: return reachableNodesAddress.Union(joining).ToList();
        case DownAll _: return nodeAddress().Union(joining).ToList();
      }

      return new List<AC.UniqueAddress>();
    }

    private void unreachableMember(AC.Member m)
    {
      if (m.UniqueAddress == SelfUniqueAddress)
      {
        return;
      }

      if (!_ignoreMemberStatus.Contains(m.Status))
      {
        _unreachable.Add(m.UniqueAddress);

        if (m.Status != AC.MemberStatus.Joining)
        {
          add(m);
        }
      }

      _stableDeadline = resetStableDeadline();
    }

    private void reachableMember(AC.Member m)
    {
      _unreachable.Remove(m.UniqueAddress);

      _stableDeadline = resetStableDeadline();
    }

    private void memberUp(AC.Member m)
    {
      add(m);

      if (m.UniqueAddress == SelfUniqueAddress)
      {
        _isSelfAdded = true;
      }

      _stableDeadline = resetStableDeadline();
    }

    private void memberRemoved(AC.Member m)
    {
      if (m.UniqueAddress == SelfUniqueAddress)
      {
        Context.Stop(Self);
      }
      else
      {
        _unreachable.Remove(m.UniqueAddress);
        _members.Remove(m);
        _stableDeadline = resetStableDeadline();
      }
    }

    private void add(AC.Member m)
    {
      _members = _members.Add(m);
    }
  }
}