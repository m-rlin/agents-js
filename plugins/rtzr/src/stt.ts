// SPDX-FileCopyrightText: 2026 LiveKit, Inc.
//
// SPDX-License-Identifier: Apache-2.0
import { type APIConnectOptions, type AudioBuffer, stt } from '@livekit/agents';
import type { AudioFrame } from '@livekit/rtc-node';
import WebSocket from 'ws';

// Constants
const DEFAULT_SAMPLE_RATE = 8000;
const DEFAULT_CHUNK_MS = 100;
const IDLE_TIMEOUT_SECONDS = 25.0;
const RECV_COMPLETION_TIMEOUT = 5000;
const IDLE_CHECK_INTERVAL = 1000;

// ============================================================================
// Types and Interfaces
// ============================================================================

interface RTZRToken {
  access_token: string;
  expire_at: number;
}

interface RTZRConfig {
  model_name: string;
  domain: string;
  sample_rate: string;
  encoding: string;
  epd_time: string;
  noise_threshold: string;
  active_threshold: string;
  use_punctuation: string;
  keywords?: string;
}

interface RTZRAlternative {
  text: string;
}

interface RTZRTranscriptResponse {
  start_at?: number;
  duration?: number;
  alternatives?: RTZRAlternative[];
  final?: boolean;
  error?: string;
  type?: string;
  message?: string;
}

type Keyword = string | [string, number];

interface STTOptionsInternal {
  model: string;
  language: string;
  sampleRate: number;
  encoding: string;
  domain: string;
  epdTime: number;
  noiseThreshold: number;
  activeThreshold: number;
  usePunctuation: boolean;
  keywords: Keyword[] | null;
}

enum StreamState {
  IDLE = 'IDLE',
  ACTIVE = 'ACTIVE',
  CLOSING = 'CLOSING',
  CLOSED = 'CLOSED',
}

// ============================================================================
// Errors
// ============================================================================

export class RTZRAPIError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RTZRAPIError';
  }
}

export class RTZRConnectionError extends RTZRAPIError {
  constructor(message: string) {
    super(message);
    this.name = 'RTZRConnectionError';
  }
}

export class RTZRStatusError extends RTZRAPIError {
  statusCode: number | null;

  constructor(message: string, statusCode: number | null = null) {
    super(message);
    this.name = 'RTZRStatusError';
    this.statusCode = statusCode;
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

function formatKeywords(keywords: Keyword[]): string {
  if (keywords.length > 100) {
    throw new Error('RTZR keyword boosting supports up to 100 keywords');
  }

  const formatted: string[] = [];

  for (const item of keywords) {
    if (Array.isArray(item)) {
      const [word, boost] = item;
      if (typeof word !== 'string') {
        throw new Error('RTZR keyword boosting keywords must be strings');
      }
      if (typeof boost !== 'number') {
        throw new Error('RTZR keyword boost must be a number');
      }
      if (!word) {
        throw new Error('RTZR keyword boosting keywords must be non-empty');
      }
      if (word.length > 20) {
        throw new Error('RTZR keyword boosting keywords must be <= 20 chars');
      }
      if (boost < -5.0 || boost > 5.0) {
        throw new Error('RTZR keyword boost must be between -5.0 and 5.0');
      }
      formatted.push(`${word}:${boost}`);
      continue;
    }

    if (typeof item !== 'string') {
      throw new Error('RTZR keyword boosting items must be strings or [keyword, boost]');
    }

    const keyword = item.trim();
    if (!keyword) {
      throw new Error('RTZR keyword boosting keywords must be non-empty');
    }

    if (keyword.includes(':')) {
      const lastColonIndex = keyword.lastIndexOf(':');
      const word = keyword.substring(0, lastColonIndex);
      const boostStr = keyword.substring(lastColonIndex + 1);

      if (!word) {
        throw new Error('RTZR keyword boosting keywords must be non-empty');
      }
      if (word.length > 20) {
        throw new Error('RTZR keyword boosting keywords must be <= 20 chars');
      }

      const boost = parseFloat(boostStr);
      if (isNaN(boost)) {
        throw new Error('RTZR keyword boost must be a number');
      }
      if (boost < -5.0 || boost > 5.0) {
        throw new Error('RTZR keyword boost must be between -5.0 and 5.0');
      }
      formatted.push(`${word}:${boost}`);
      continue;
    }

    if (keyword.length > 20) {
      throw new Error('RTZR keyword boosting keywords must be <= 20 chars');
    }
    formatted.push(keyword);
  }

  return formatted.join(',');
}

// ============================================================================
// RTZR OpenAPI Client
// ============================================================================

export class RTZROpenAPIClient {
  private clientId: string;
  private clientSecret: string;
  private token: RTZRToken | null = null;
  private apiBase = 'https://openapi.vito.ai';
  private wsBase = 'wss://openapi.vito.ai';

  constructor(clientId?: string, clientSecret?: string) {
    this.clientId = clientId || process.env.RTZR_CLIENT_ID || '';
    this.clientSecret = clientSecret || process.env.RTZR_CLIENT_SECRET || '';

    if (!this.clientId || !this.clientSecret) {
      throw new Error('RTZR_CLIENT_ID and RTZR_CLIENT_SECRET must be set');
    }
  }

  async getToken(): Promise<string> {
    const now = Date.now() / 1000;
    if (this.token === null || this.token.expire_at < now - 3600) {
      await this.refreshToken();
    }
    if (this.token === null) {
      throw new RTZRAPIError('Failed to obtain RTZR access token');
    }
    return this.token.access_token;
  }

  private async refreshToken(): Promise<void> {
    const url = `${this.apiBase}/v1/authenticate`;

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
          client_id: this.clientId,
          client_secret: this.clientSecret,
        }),
      });

      if (!response.ok) {
        throw new RTZRStatusError(`Authentication failed: ${response.statusText}`, response.status);
      }

      const data = await response.json();
      if (typeof data.access_token !== 'string' || typeof data.expire_at !== 'number') {
        throw new RTZRStatusError('Invalid token response payload');
      }

      this.token = {
        access_token: data.access_token,
        expire_at: data.expire_at,
      };
      console.log('[RTZR] Successfully refreshed access token');
    } catch (error) {
      if (error instanceof RTZRAPIError) {
        throw error;
      }
      throw new RTZRConnectionError(`Failed to authenticate with RTZR API: ${error}`);
    }
  }

  buildConfig(options: {
    modelName?: string;
    domain?: string;
    sampleRate?: number;
    encoding?: string;
    epdTime?: number;
    noiseThreshold?: number;
    activeThreshold?: number;
    usePunctuation?: boolean;
    keywords?: Keyword[] | null;
  }): RTZRConfig {
    const config: RTZRConfig = {
      model_name: options.modelName || 'sommers_ko',
      domain: options.domain || 'CALL',
      sample_rate: String(options.sampleRate || DEFAULT_SAMPLE_RATE),
      encoding: options.encoding || 'LINEAR16',
      epd_time: String(options.epdTime || 0.5),
      noise_threshold: String(options.noiseThreshold || 0.6),
      active_threshold: String(options.activeThreshold || 0.8),
      use_punctuation: options.usePunctuation ? 'true' : 'false',
    };

    if (options.keywords && options.keywords.length > 0) {
      config.keywords = formatKeywords(options.keywords);
    }

    return config;
  }

  getWebSocketUrl(config: RTZRConfig): string {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(config)) {
      if (value !== undefined) {
        params.set(key, value);
      }
    }
    const url = `${this.wsBase}/v1/transcribe:streaming?${params.toString()}`;
    console.log(`[RTZR] WebSocket URL: ${url}`);
    return url;
  }
}

// ============================================================================
// Audio Buffer for chunking
// ============================================================================

class AudioByteStream {
  private buffer: Buffer = Buffer.alloc(0);
  private bytesPerFrame: number;

  constructor(_sampleRate: number, numChannels: number, samplesPerChannel: number) {
    // 16-bit PCM = 2 bytes per sample
    this.bytesPerFrame = samplesPerChannel * numChannels * 2;
  }

  write(data: Buffer): Buffer[] {
    this.buffer = Buffer.concat([this.buffer, data]);
    const frames: Buffer[] = [];

    while (this.buffer.length >= this.bytesPerFrame) {
      frames.push(this.buffer.subarray(0, this.bytesPerFrame));
      this.buffer = this.buffer.subarray(this.bytesPerFrame);
    }

    return frames;
  }

  flush(): Buffer[] {
    if (this.buffer.length > 0) {
      const remaining = this.buffer;
      this.buffer = Buffer.alloc(0);
      return [remaining];
    }
    return [];
  }
}

// ============================================================================
// STT Class
// ============================================================================

export interface STTOptions {
  model?: string;
  language?: string;
  sampleRate?: number;
  domain?: string;
  epdTime?: number;
  noiseThreshold?: number;
  activeThreshold?: number;
  usePunctuation?: boolean;
  keywords?: Keyword[] | null;
}
export type RTZRSTTOptions = STTOptions;

export class STT extends stt.STT {
  label = 'rtzr.STT';
  private params: STTOptionsInternal;
  private _client: RTZROpenAPIClient;

  constructor(options: STTOptions = {}) {
    super({
      streaming: true,
      interimResults: true,
      alignedTranscript: 'chunk',
    });

    this.params = {
      model: options.model || 'sommers_ko',
      language: options.language || 'ko',
      sampleRate: options.sampleRate || DEFAULT_SAMPLE_RATE,
      encoding: 'LINEAR16',
      domain: options.domain || 'CALL',
      epdTime: options.epdTime || 0.8,
      noiseThreshold: options.noiseThreshold || 0.6,
      activeThreshold: options.activeThreshold || 0.8,
      usePunctuation: options.usePunctuation || false,
      keywords: options.keywords || null,
    };

    if (options.keywords && options.model !== 'sommers_ko') {
      console.warn('[RTZR] Keyword boosting is only supported with sommers_ko model');
    }

    this._client = new RTZROpenAPIClient();
    console.log(
      `[RTZR] STT initialized: model=${this.params.model}, sampleRate=${this.params.sampleRate}, language=${this.params.language}`,
    );
  }

  get sampleRate(): number {
    return this.params.sampleRate;
  }

  get client(): RTZROpenAPIClient {
    return this._client;
  }

  get options(): STTOptionsInternal {
    return this.params;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected async _recognize(
    _frame: AudioBuffer,
    _abortSignal?: AbortSignal,
  ): Promise<stt.SpeechEvent> {
    throw new Error('RTZR STT does not support batch recognition, use stream() instead');
  }

  stream(options?: { connOptions?: APIConnectOptions }): SpeechStream {
    return new SpeechStream(this, options?.connOptions);
  }
}

// ============================================================================
// SpeechStream Class
// ============================================================================

export class SpeechStream extends stt.SpeechStream {
  label = 'rtzr.SpeechStream';
  private rtzrStt: STT;
  private ws: WebSocket | null = null;
  private state: StreamState = StreamState.IDLE;
  private lastAudioAt: number | null = null;
  private idleCheckInterval: ReturnType<typeof setInterval> | null = null;
  private audioBuffer: AudioByteStream;
  private inSpeech = false;
  private speechStartedAt: number | null = null;
  private resolveClose: (() => void) | null = null;

  constructor(sttInstance: STT, connOptions?: APIConnectOptions) {
    super(sttInstance, sttInstance.sampleRate, connOptions);
    this.rtzrStt = sttInstance;

    const samplesPerChunk = Math.floor(sttInstance.sampleRate / (1000 / DEFAULT_CHUNK_MS));
    this.audioBuffer = new AudioByteStream(sttInstance.sampleRate, 1, samplesPerChunk);
  }

  protected async run(): Promise<void> {
    this.startIdleWatchdog();
    console.log('[RTZR] SpeechStream run() started');

    try {
      let frameCount = 0;
      for await (const data of this.input) {
        if (this.closed) break;

        if (data === SpeechStream.FLUSH_SENTINEL) {
          console.log('[RTZR] Received FLUSH_SENTINEL');
          const frames = this.audioBuffer.flush();
          for (const frame of frames) {
            await this.sendAudioFrame(frame);
          }
          await this.endSegment();
          continue;
        }

        const frame = data as AudioFrame;
        frameCount++;
        if (frameCount === 1) {
          console.log(
            `[RTZR] First audio frame received: sampleRate=${frame.sampleRate}, channels=${frame.channels}, samples=${frame.samplesPerChannel}`,
          );
        }
        if (frameCount % 100 === 0) {
          console.log(`[RTZR] Processed ${frameCount} frames`);
        }
        // Convert Int16Array to Buffer correctly (respecting byteOffset and byteLength)
        const audioData = Buffer.from(
          frame.data.buffer,
          frame.data.byteOffset,
          frame.data.byteLength,
        );
        const chunks = this.audioBuffer.write(audioData);

        if (chunks.length > 0 && !this.ws) {
          try {
            await this.ensureConnected();
          } catch (error) {
            console.error('[RTZR] Failed to connect WebSocket:', error);
            throw error;
          }
        }

        for (const chunk of chunks) {
          await this.sendAudioFrame(chunk);
        }
      }

      // Final shutdown
      if (this.ws) {
        this.state = StreamState.CLOSING;
        try {
          this.ws.send('EOS');
          console.log('[RTZR] Sent final EOS to close audio stream');
        } catch (error) {
          console.error('[RTZR] Failed to send final EOS:', error);
        }
        await this.awaitRecvCompletion();
        await this.cleanupConnection();
      }
    } finally {
      this.stopIdleWatchdog();
      this.state = StreamState.CLOSED;
    }
  }

  private async sendAudioFrame(data: Buffer): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      this.lastAudioAt = Date.now();
    }
  }

  private async ensureConnected(): Promise<void> {
    if (this.ws !== null) return;

    const config = this.rtzrStt.client.buildConfig({
      modelName: this.rtzrStt.options.model,
      domain: this.rtzrStt.options.domain,
      sampleRate: this.rtzrStt.options.sampleRate,
      encoding: this.rtzrStt.options.encoding,
      epdTime: this.rtzrStt.options.epdTime,
      noiseThreshold: this.rtzrStt.options.noiseThreshold,
      activeThreshold: this.rtzrStt.options.activeThreshold,
      usePunctuation: this.rtzrStt.options.usePunctuation,
      keywords: this.rtzrStt.options.keywords,
    });

    const token = await this.rtzrStt.client.getToken();
    const url = this.rtzrStt.client.getWebSocketUrl(config);

    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(url, {
          headers: {
            Authorization: `bearer ${token}`,
          },
        });

        this.ws.binaryType = 'arraybuffer';

        this.ws.on('open', () => {
          console.log(
            `[RTZR] WebSocket connected (model=${this.rtzrStt.options.model}, sr=${this.rtzrStt.options.sampleRate})`,
          );
          this.state = StreamState.ACTIVE;
          this.lastAudioAt = Date.now();
          resolve();
        });

        this.ws.on('message', (data: WebSocket.Data) => {
          this.handleMessage(data);
        });

        this.ws.on('error', (error: Error) => {
          console.error('[RTZR] WebSocket error:', error);
          if (this.state === StreamState.IDLE) {
            reject(new RTZRConnectionError('WebSocket connection failed'));
          }
        });

        this.ws.on('close', () => {
          console.log('[RTZR] WebSocket closed');
          if (this.resolveClose) {
            this.resolveClose();
            this.resolveClose = null;
          }
          this.ws = null;
          if (this.state !== StreamState.CLOSED) {
            this.state = StreamState.IDLE;
          }
        });
      } catch (error) {
        this.state = StreamState.IDLE;
        reject(error);
      }
    });
  }

  private handleMessage(rawData: WebSocket.Data): void {
    const dataStr = rawData.toString();
    console.log('[RTZR] Received message:', dataStr.substring(0, 200));

    let data: RTZRTranscriptResponse;
    try {
      data = JSON.parse(dataStr);
    } catch {
      console.warn('[RTZR] Non-JSON text received:', dataStr);
      return;
    }

    // Check for errors
    if (data.error) {
      console.error('[RTZR] Server error:', data.error);
      return;
    }
    if (data.type === 'error' && data.message) {
      console.error('[RTZR] Server error:', data.message);
      return;
    }

    // Process transcript
    const events = this.processTranscriptEvent(data);
    console.log(`[RTZR] Generated ${events.length} events from transcript`);
    for (const evt of events) {
      if (!this.queue.closed) {
        this.queue.put(evt);
      }
    }
  }

  private processTranscriptEvent(data: RTZRTranscriptResponse): stt.SpeechEvent[] {
    const startTime = (data.start_at || 0) / 1000.0;
    const duration = (data.duration || 0) / 1000.0;

    if (!data.alternatives || data.alternatives.length === 0) {
      return [];
    }

    const alternative = data.alternatives[0];
    if (!alternative) {
      return [];
    }

    const text = alternative.text || '';
    const isFinal = Boolean(data.final);

    if (!text) {
      return [];
    }

    const events: stt.SpeechEvent[] = [];

    if (!this.inSpeech) {
      this.inSpeech = true;
      this.speechStartedAt = Date.now();
      events.push({
        type: stt.SpeechEventType.START_OF_SPEECH,
      });
    }

    const eventType = isFinal
      ? stt.SpeechEventType.FINAL_TRANSCRIPT
      : stt.SpeechEventType.INTERIM_TRANSCRIPT;

    events.push({
      type: eventType,
      alternatives: [
        {
          text,
          language: this.rtzrStt.options.language,
          startTime: startTime + this.startTimeOffset,
          endTime: startTime + duration + this.startTimeOffset,
          confidence: 1.0,
        },
      ],
    });

    if (isFinal) {
      const durationMs = this.speechStartedAt ? Date.now() - this.speechStartedAt : 0;
      console.log(
        `[RTZR] Final transcript received (speech_duration=${(durationMs / 1000).toFixed(2)}s)`,
      );
      events.push({
        type: stt.SpeechEventType.END_OF_SPEECH,
      });

      // Emit metrics directly to bypass base monitorMetrics() which hardcodes durationMs: 0
      this.rtzrStt.emit('metrics_collected', {
        type: 'stt_metrics' as const,
        timestamp: Date.now(),
        requestId: `rtzr-${Date.now()}`,
        durationMs,
        label: this.rtzrStt.label,
        audioDurationMs: Math.round(duration * 1000),
        streamed: true,
      });

      this.inSpeech = false;
      this.speechStartedAt = null;
    }

    return events;
  }

  private async endSegment(): Promise<void> {
    if (!this.ws) return;

    this.state = StreamState.CLOSING;
    try {
      this.ws.send('EOS');
      console.log('[RTZR] Sent EOS to close audio segment');
    } catch (error) {
      console.error('[RTZR] Failed to send EOS:', error);
    }

    await this.awaitRecvCompletion();
    await this.cleanupConnection();
    this.state = StreamState.IDLE;
    this.lastAudioAt = null;
  }

  private startIdleWatchdog(): void {
    this.idleCheckInterval = setInterval(() => {
      if (this.state === StreamState.CLOSED) {
        this.stopIdleWatchdog();
        return;
      }

      if (this.state !== StreamState.ACTIVE || !this.ws) return;
      if (this.lastAudioAt === null) return;

      const idleTime = (Date.now() - this.lastAudioAt) / 1000;
      if (idleTime >= IDLE_TIMEOUT_SECONDS) {
        console.log(`[RTZR] Idle timeout reached (${IDLE_TIMEOUT_SECONDS}s); closing segment`);
        void this.endSegment();
      }
    }, IDLE_CHECK_INTERVAL);
  }

  private stopIdleWatchdog(): void {
    if (this.idleCheckInterval) {
      clearInterval(this.idleCheckInterval);
      this.idleCheckInterval = null;
    }
  }

  private async awaitRecvCompletion(): Promise<void> {
    if (!this.ws) return;

    return new Promise<void>((resolve) => {
      this.resolveClose = resolve;

      setTimeout(() => {
        if (this.resolveClose) {
          this.resolveClose();
          this.resolveClose = null;
        }
      }, RECV_COMPLETION_TIMEOUT);
    });
  }

  private async cleanupConnection(): Promise<void> {
    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        // Ignore close errors
      }
      this.ws = null;
    }
  }
}

// Named exports
export { STT as RTZRSTT, SpeechStream as RTZRSpeechStream };
