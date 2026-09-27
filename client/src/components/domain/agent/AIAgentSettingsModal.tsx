import { useState, useEffect } from "react";
import { Bot, Key, Sparkles, PhoneCall, Volume2, Save, CheckCircle2 } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { useAgentStore } from "@/stores/agent";
import { toast } from "sonner";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const PRESET_MODELS = [
  { id: "openrouter/auto", name: "Auto Router (Recommended)", desc: "Best available low-latency model" },
  { id: "google/gemini-2.0-flash-001", name: "Gemini 2.0 Flash", desc: "Ultra-fast response for voice" },
  { id: "deepseek/deepseek-chat", name: "DeepSeek V3", desc: "Smart conversational AI" },
  { id: "meta-llama/llama-3.3-70b-instruct", name: "Llama 3.3 70B", desc: "Open-weight reasoning" },
  { id: "openai/gpt-4o-mini", name: "GPT-4o Mini", desc: "Fast and lightweight" },
];

const PRESET_VOICES = [
  { id: "dialog-nipunika", name: "Dialog Nipunika (Studio Natural Sinhala Female 🇱🇰 ✨)" },
  { id: "piper-openslr", name: "OpenSLR 30 (Sinhala Female 🇱🇰)" },
  { id: "piper-ashoka", name: "Ashoka (Sinhala Male 🇱🇰)" },
  { id: "si-LK-ThiliniNeural", name: "Thilini (Edge Sinhala Female 🇱🇰)" },
  { id: "si-LK-SameeraNeural", name: "Sameera (Edge Sinhala Male 🇱🇰)" },
  { id: "en-US-JennyNeural", name: "Jenny (US English Female)" },
  { id: "en-US-GuyNeural", name: "Guy (US English Male)" },
];

export const AIAgentSettingsModal = ({ open, onOpenChange }: Props) => {
  const config = useAgentStore((s) => s.config);
  const updateConfig = useAgentStore((s) => s.updateConfig);

  const [enabled, setEnabled] = useState(config.enabled);
  const [autoAnswer, setAutoAnswer] = useState(config.autoAnswer);
  const [apiKey, setApiKey] = useState("");
  const [model, setModel] = useState(config.model);
  const [systemPrompt, setSystemPrompt] = useState(config.systemPrompt);
  const [voice, setVoice] = useState(config.voice);
  const [saving, setSaving] = useState(false);

  const [azureKey, setAzureKey] = useState("");
  const [azureRegion, setAzureRegion] = useState(config.azureSpeechRegion || "eastus");
  const [googleKey, setGoogleKey] = useState("");
  const [hfToken, setHfToken] = useState("");
  const [customTtsUrl, setCustomTtsUrl] = useState(config.customTtsUrl || "");
  const [customTtsKey, setCustomTtsKey] = useState("");

  useEffect(() => {
    setEnabled(config.enabled);
    setAutoAnswer(config.autoAnswer);
    setModel(config.model);
    setSystemPrompt(config.systemPrompt);
    setVoice(config.voice);
    setAzureRegion(config.azureSpeechRegion || "eastus");
    setCustomTtsUrl(config.customTtsUrl || "");
  }, [config]);

  const handleSave = async () => {
    setSaving(true);
    try {
      const patch: any = {
        enabled,
        autoAnswer,
        model,
        systemPrompt,
        voice,
        azureSpeechRegion: azureRegion,
        customTtsUrl,
      };
      if (apiKey.trim()) {
        patch.openRouterKey = apiKey.trim();
      }
      if (azureKey.trim()) {
        patch.azureSpeechKey = azureKey.trim();
      }
      if (googleKey.trim()) {
        patch.googleCloudKey = googleKey.trim();
      }
      if (hfToken.trim()) {
        patch.hfToken = hfToken.trim();
      }
      if (customTtsKey.trim()) {
        patch.customTtsKey = customTtsKey.trim();
      }
      await updateConfig(patch);
      toast.success("AI Agent settings updated successfully");
      onOpenChange(false);
      setApiKey("");
      setAzureKey("");
      setGoogleKey("");
      setHfToken("");
      setCustomTtsKey("");
    } catch {
      toast.error("Failed to update AI Agent settings");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <div className="flex items-center gap-2">
            <div className="p-2 rounded-lg bg-emerald-500/10 text-emerald-500">
              <Bot className="h-6 w-6" />
            </div>
            <div>
              <DialogTitle className="text-xl flex items-center gap-2">
                AI Voice Agent Settings
                {config.hasKey && (
                  <Badge variant="secondary" className="text-xs bg-emerald-500/10 text-emerald-600 border-emerald-500/20">
                    <CheckCircle2 className="h-3 w-3 mr-1" /> Ready
                  </Badge>
                )}
              </DialogTitle>
              <DialogDescription>
                Configure OpenRouter AI intelligence, voice synthesis, and autonomous call behaviors.
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        <div className="space-y-6 py-2">
          {/* Toggles */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div
              onClick={() => setEnabled(!enabled)}
              className={`p-4 rounded-xl border cursor-pointer transition-all ${
                enabled ? "border-emerald-500 bg-emerald-500/5" : "border-border hover:border-border/80"
              }`}
            >
              <div className="flex items-center justify-between">
                <span className="font-semibold text-sm flex items-center gap-2">
                  <Bot className="h-4 w-4 text-emerald-500" />
                  AI Voice Assistant
                </span>
                <span
                  className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                    enabled ? "bg-emerald-500 text-white" : "bg-muted text-muted-foreground"
                  }`}
                >
                  {enabled ? "Enabled" : "Disabled"}
                </span>
              </div>
              <p className="text-xs text-muted-foreground mt-2">
                Allows AI Agent to process call audio and speak on active WhatsApp calls.
              </p>
            </div>

            <div
              onClick={() => setAutoAnswer(!autoAnswer)}
              className={`p-4 rounded-xl border cursor-pointer transition-all ${
                autoAnswer ? "border-emerald-500 bg-emerald-500/5" : "border-border hover:border-border/80"
              }`}
            >
              <div className="flex items-center justify-between">
                <span className="font-semibold text-sm flex items-center gap-2">
                  <PhoneCall className="h-4 w-4 text-emerald-500" />
                  Auto-Answer Calls
                </span>
                <span
                  className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                    autoAnswer ? "bg-emerald-500 text-white" : "bg-muted text-muted-foreground"
                  }`}
                >
                  {autoAnswer ? "Auto On" : "Off"}
                </span>
              </div>
              <p className="text-xs text-muted-foreground mt-2">
                Automatically accepts incoming WhatsApp calls and routes them to the AI agent.
              </p>
            </div>
          </div>

          {/* OpenRouter API Key */}
          <div className="space-y-2">
            <Label className="text-sm font-medium flex items-center gap-2">
              <Key className="h-4 w-4 text-muted-foreground" />
              OpenRouter API Key
            </Label>
            <div className="flex gap-2">
              <Input
                type="password"
                placeholder={config.hasKey ? `Current Key: ${config.openRouterKey}` : "sk-or-v1-..."}
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                className="font-mono text-xs"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Powered by OpenRouter.ai gateway. The key is securely stored in the server runtime.
            </p>
          </div>

          {/* Model Selection */}
          <div className="space-y-2">
            <Label className="text-sm font-medium flex items-center gap-2">
              <Sparkles className="h-4 w-4 text-muted-foreground" />
              AI Model
            </Label>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              {PRESET_MODELS.map((m) => (
                <div
                  key={m.id}
                  onClick={() => setModel(m.id)}
                  className={`p-2.5 rounded-lg border text-left cursor-pointer transition-colors ${
                    model === m.id ? "border-emerald-500 bg-emerald-500/10" : "border-border hover:bg-muted/50"
                  }`}
                >
                  <div className="font-medium text-xs text-foreground">{m.name}</div>
                  <div className="text-[11px] text-muted-foreground">{m.desc}</div>
                </div>
              ))}
            </div>
            <Input
              placeholder="Or custom model ID (e.g. google/gemini-2.5-flash)"
              value={model}
              onChange={(e) => setModel(e.target.value)}
              className="text-xs mt-2"
            />
          </div>

          {/* Voice Selection */}
          <div className="space-y-2">
            <Label className="text-sm font-medium flex items-center gap-2">
              <Volume2 className="h-4 w-4 text-muted-foreground" />
              Speech Voice
            </Label>
            <div className="grid grid-cols-2 gap-2">
              {PRESET_VOICES.map((v) => (
                <div
                  key={v.id}
                  onClick={() => setVoice(v.id)}
                  className={`p-2.5 rounded-lg border text-left cursor-pointer transition-colors ${
                    voice === v.id ? "border-emerald-500 bg-emerald-500/10" : "border-border hover:bg-muted/50"
                  }`}
                >
                  <div className="font-medium text-xs text-foreground">{v.name}</div>
                </div>
              ))}
            </div>
          </div>

          {/* High-Definition Human Neural Voice Providers */}
          <div className="space-y-3 p-3.5 rounded-xl border border-primary/20 bg-primary/5">
            <div className="flex items-center justify-between">
              <span className="font-semibold text-xs flex items-center gap-1.5 text-foreground">
                <Volume2 className="h-4 w-4 text-primary" />
                Human-Like HD Neural TTS Providers (Optional)
              </span>
              {config.hasAzureKey ? (
                <Badge variant="secondary" className="text-[10px] bg-emerald-500/10 text-emerald-600">Azure Active</Badge>
              ) : config.hasHfToken ? (
                <Badge variant="secondary" className="text-[10px] bg-emerald-500/10 text-emerald-600">HuggingFace MMS Active</Badge>
              ) : config.hasGoogleKey ? (
                <Badge variant="secondary" className="text-[10px] bg-emerald-500/10 text-emerald-600">Google Active</Badge>
              ) : null}
            </div>
            <p className="text-[11px] text-muted-foreground">
              For human-like studio Sinhala speech (such as Thilini and Sameera), connect an official Azure Speech key, free HuggingFace Token (Meta MMS Sinhala), or Google Cloud key.
            </p>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 pt-1">
              <div>
                <Label className="text-xs">HuggingFace Token (100% Free - Meta MMS 🇱🇰)</Label>
                <Input
                  type="password"
                  placeholder={config.hasHfToken ? config.hfToken : "hf_... (Free from huggingface.co)"}
                  value={hfToken}
                  onChange={(e) => setHfToken(e.target.value)}
                  className="font-mono text-xs mt-1"
                />
              </div>
              <div>
                <Label className="text-xs">Azure Speech Key (Best for Sinhala 🇱🇰)</Label>
                <Input
                  type="password"
                  placeholder={config.hasAzureKey ? config.azureSpeechKey : "Azure Speech subscription key"}
                  value={azureKey}
                  onChange={(e) => setAzureKey(e.target.value)}
                  className="font-mono text-xs mt-1"
                />
              </div>
              <div>
                <Label className="text-xs">Azure Region</Label>
                <Input
                  placeholder="e.g. eastus or southeastasia"
                  value={azureRegion}
                  onChange={(e) => setAzureRegion(e.target.value)}
                  className="text-xs mt-1"
                />
              </div>
              <div>
                <Label className="text-xs">Google Cloud TTS Key</Label>
                <Input
                  type="password"
                  placeholder={config.hasGoogleKey ? config.googleCloudKey : "Google Cloud API Key"}
                  value={googleKey}
                  onChange={(e) => setGoogleKey(e.target.value)}
                  className="font-mono text-xs mt-1"
                />
              </div>
              <div>
                <Label className="text-xs">Custom TTS Server URL (OpenAI format)</Label>
                <Input
                  placeholder="https://.../v1/audio/speech"
                  value={customTtsUrl}
                  onChange={(e) => setCustomTtsUrl(e.target.value)}
                  className="text-xs mt-1"
                />
              </div>
              <div>
                <Label className="text-xs">Custom TTS Server Key</Label>
                <Input
                  type="password"
                  placeholder={config.hasCustomTtsKey ? config.customTtsKey : "Bearer token / key"}
                  value={customTtsKey}
                  onChange={(e) => setCustomTtsKey(e.target.value)}
                  className="font-mono text-xs mt-1"
                />
              </div>
            </div>
          </div>

          {/* System Prompt / Persona */}
          <div className="space-y-2">
            <Label className="text-sm font-medium">System Instructions / Persona</Label>
            <textarea
              rows={3}
              value={systemPrompt}
              onChange={(e) => setSystemPrompt(e.target.value)}
              className="w-full text-xs p-3 rounded-lg border border-input bg-background focus:outline-none focus:ring-2 focus:ring-emerald-500"
              placeholder="Describe how the AI should behave during the call..."
            />
          </div>
        </div>

        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={saving} className="bg-emerald-600 hover:bg-emerald-700 text-white">
            <Save className="h-4 w-4 mr-1.5" />
            {saving ? "Saving..." : "Save Configuration"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
