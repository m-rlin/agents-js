// SPDX-FileCopyrightText: 2026 LiveKit, Inc.
//
// SPDX-License-Identifier: Apache-2.0
import { Plugin } from '@livekit/agents';

export * from './stt.js';

class RTZRPlugin extends Plugin {
  constructor() {
    super({
      title: 'rtzr',
      version: '0.1.0',
      package: '@livekit/agents-plugin-rtzr',
    });
  }
}

Plugin.registerPlugin(new RTZRPlugin());
