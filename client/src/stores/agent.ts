import { create } from "zustand";
import { eventStream, type BrokerEvent, type AgentConfigData } from "@/lib/event-stream";

export type TranscriptItem = {
  role: "user" | "assistant" | "system";
  text: string;
  timestamp: number;
};

export type AgentCallState = {
  enabled: boolean;
  state: "idle" | "listening" | "thinking" | "speaking";
};

type State = {
  config: AgentConfigData;
  transcripts: Record<string, TranscriptItem[]>;
  callStates: Record<string, AgentCallState>;
  fetchConfig: () => Promise<void>;
  updateConfig: (patch: Partial<AgentConfigData>) => Promise<void>;
  toggleCallAgent: (sessionId: string, callId: string, enabled: boolean) => Promise<void>;
};

const defaultConfig: AgentConfigData = {
  enabled: true,
  autoAnswer: false,
  openRouterKey: "",
  hasKey: false,
  model: "openrouter/auto",
  systemPrompt: "ඔබ ඉතා දක්ෂ, මිත්‍රශීලී සහ කාරුණික AI හඬ සහායකයෙකි. ඔබ සජීවී WhatsApp දුරකථන ඇමතුමකට පිළිතුරු දෙයි. සැමවිටම ඉතා පැහැදිලි, ස්වාභාවික සහ කාරුණික කතාබහ කරන සිංහල භාෂාවෙන් (හෝ අමතන්නා ඉංග්‍රීසියෙන් කතා කළහොත් ඉංග්‍රීසියෙන්) ඉතා කෙටියෙන් (වාක්‍ය 1-2 කින්) පිළිතුරු දෙන්න. කිසිවිටෙකත් markdown, bullet points, තරු ලකුණු හෝ emojis භාවිතා නොකරන්න. කටහඬින් කතා කරන ආකාරයටම ස්වාභාවිකව පිළිතුරු දෙන්න.",
  voice: "dialog-nipunika",
};

export const useAgentStore = create<State>((set, get) => ({
  config: defaultConfig,
  transcripts: {},
  callStates: {},

  fetchConfig: async () => {
    try {
      const res = await fetch("/api/agent/config");
      if (res.ok) {
        const data = await res.json();
        set({ config: { ...defaultConfig, ...data } });
      }
    } catch {}
  },

  updateConfig: async (patch: Partial<AgentConfigData>) => {
    try {
      const res = await fetch("/api/agent/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      });
      if (res.ok) {
        const data = await res.json();
        set({ config: { ...get().config, ...data } });
      }
    } catch {}
  },

  toggleCallAgent: async (sessionId: string, callId: string, enabled: boolean) => {
    try {
      set((s) => ({
        callStates: {
          ...s.callStates,
          [callId]: {
            enabled,
            state: enabled ? "idle" : "idle",
          },
        },
      }));
      await fetch(`/api/sessions/${sessionId}/calls/${callId}/agent`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled }),
      });
    } catch {}
  },
}));

let wired = false;
export const ensureAgentWired = (): void => {
  if (wired) return;
  wired = true;

  void useAgentStore.getState().fetchConfig();

  eventStream.on((ev: BrokerEvent) => {
    if (ev.type === "agent-config") {
      useAgentStore.setState({ config: ev.config });
    } else if (ev.type === "agent-transcript") {
      useAgentStore.setState((s) => {
        const list = s.transcripts[ev.callId] || [];
        return {
          transcripts: {
            ...s.transcripts,
            [ev.callId]: [
              ...list,
              { role: ev.role, text: ev.text, timestamp: ev.timestamp },
            ],
          },
        };
      });
    } else if (ev.type === "agent-status") {
      useAgentStore.setState((s) => ({
        callStates: {
          ...s.callStates,
          [ev.callId]: {
            enabled: ev.enabled,
            state: (ev.state as AgentCallState["state"]) || "idle",
          },
        },
      }));
    } else if (ev.type === "call-ended") {
      useAgentStore.setState((s) => {
        const nextStates = { ...s.callStates };
        delete nextStates[ev.id];
        return { callStates: nextStates };
      });
    }
  });
};
