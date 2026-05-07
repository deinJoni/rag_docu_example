export type RawFile = {
  path: string;
  size: number;
  contentType: string | null;
  updatedAt: string;
};

export type ParsedDocument = {
  id: string;
  source: string;
  content: string;
  metadata: Record<string, unknown>;
};

export type EmbeddedDocument = ParsedDocument & {
  embedding: number[];
};
