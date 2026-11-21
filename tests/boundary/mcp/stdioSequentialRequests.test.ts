import { PassThrough } from "node:stream";
import {
  Client,
  ReadBuffer,
  serializeMessage,
  type JSONRPCMessage,
  type Transport,
} from "@modelcontextprotocol/client";
import {
  serveStdio,
  StdioServerTransport,
} from "@modelcontextprotocol/server/stdio";
import pino from "pino";
import { expect, it } from "vitest";

import { createManagedBinarySession } from "../../../src/composition/binary.js";
import { createServer } from "../../../src/server/createServer.js";
import { createToolResultDelivery } from "../../../src/server/toolResult.js";
import { startMcpTransport } from "../../../src/main/transport.js";

/** Client transport over the same pipes the production stdio server reads and writes. */
class PipeClientTransport implements Transport {
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: (message: JSONRPCMessage) => void;
  private readonly buffer = new ReadBuffer();

  constructor(
    private readonly toServer: PassThrough,
    private readonly fromServer: PassThrough,
  ) {}

  async start(): Promise<void> {
    this.fromServer.on("data", (chunk: Buffer) => {
      this.buffer.append(chunk);
      for (;;) {
        const message = this.buffer.readMessage();
        if (message === null) return;
        this.onmessage?.(message);
      }
    });
  }

  async send(message: JSONRPCMessage): Promise<void> {
    this.toServer.write(serializeMessage(message));
  }

  async close(): Promise<void> {
    this.onclose?.();
  }
}

/** Send a raw server/discover, as a probing client does before it falls back. */
const probeDiscover = async (
  toServer: PassThrough,
  fromServer: PassThrough,
): Promise<void> => {
  const answered = new Promise<string>((resolve) =>
    fromServer.once("data", (chunk: Buffer) => resolve(String(chunk))),
  );
  toServer.write(
    `${JSON.stringify({
      jsonrpc: "2.0",
      id: "probe",
      method: "server/discover",
      params: {
        _meta: {
          "io.modelcontextprotocol/protocolVersion": "2026-07-28",
          "io.modelcontextprotocol/clientCapabilities": {},
        },
      },
    })}\n`,
  );
  expect(JSON.parse(await answered)).toMatchObject({
    id: "probe",
    result: { supportedVersions: ["2026-07-28"] },
  });
};

const CHEAP_READ_ONLY_TOOL = "get_evidence_bundle";

// Two requests in a row on one connection is the regression: TypeScript server
// 2.3.0 broke a shared server object only on the second request. Both protocol
// eras must be served by the production factory path.
it.each([
  { name: "legacy", era: "legacy", mode: "legacy", probe: false },
  { name: "modern", era: "modern", mode: { pin: "2026-07-28" }, probe: false },
  // Claude Code sends server/discover and then falls back to initialize; the
  // SDK discards the probe server and builds a replacement from the factory.
  {
    name: "legacy after discover probe",
    era: "legacy",
    mode: "legacy",
    probe: true,
  },
] as const)(
  "serves tools/list and repeated tools/call on one stdio connection: $name",
  async ({ era, mode, probe }) => {
    const toServer = new PassThrough();
    const fromServer = new PassThrough();
    const logger = pino({ enabled: false });
    const session = createManagedBinarySession();
    const started = await startMcpTransport(
      {
        env: {},
        serve: (factory, options) =>
          serveStdio(factory, {
            ...options,
            transport: new StdioServerTransport(toServer, fromServer),
          }),
        writeStderr: () => undefined,
        setExitCode: () => undefined,
        registerShutdown: () => () => undefined,
        createServer,
      },
      session,
      {
        environment: {},
        delivery: createToolResultDelivery(undefined),
        logger,
        serverLogger: logger,
        loadOptionalProviders: async () => ({}),
      },
    );
    if (!started.ok) throw new Error("Transport startup failed");
    const client = new Client(
      { name: `sequential-${era}`, version: "1.0.0" },
      { versionNegotiation: { mode } },
    );
    try {
      if (probe) await probeDiscover(toServer, fromServer);
      await client.connect(new PipeClientTransport(toServer, fromServer));
      expect(client.getProtocolEra()).toBe(era);
      expect(client.getServerVersion()?.name).toBe("rea");
      if (era === "modern")
        expect(client.getDiscoverResult()?.supportedVersions).toContain(
          "2026-07-28",
        );

      const listed = await client.listTools();
      expect(listed.tools.map((tool) => tool.name)).toContain(
        CHEAP_READ_ONLY_TOOL,
      );
      for (let call = 0; call < 2; call += 1) {
        const result = await client.callTool({
          name: CHEAP_READ_ONLY_TOOL,
          arguments: {},
        });
        expect(result.isError, `call ${String(call + 1)}`).not.toBe(true);
        expect(
          result.structuredContent,
          `call ${String(call + 1)}`,
        ).toBeTruthy();
      }
    } finally {
      await client.close();
      await started.handle.close();
      await started.closeAndroid();
      await session.close();
      toServer.destroy();
      fromServer.destroy();
    }
  },
);
