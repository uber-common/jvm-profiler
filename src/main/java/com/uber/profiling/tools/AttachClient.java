package com.uber.profiling.tools;

import com.sun.tools.attach.VirtualMachine;

public class AttachClient {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Usage: com.uber.profiling.tools.AttachClient <agent_jar_full_path> <agent_args> <vm_pid>");
      return;
    }
    String agent_path = args[0];
    String agent_args = args[1];
    String pid = args[2];
    VirtualMachine vm = VirtualMachine.attach(pid);
    System.out.println("vm = " + vm);
    vm.loadAgent(agent_path, agent_args);
    vm.detach();
  }
}
