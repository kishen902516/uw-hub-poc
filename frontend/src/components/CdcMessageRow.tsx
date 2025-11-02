'use client';

import { TableRow, TableCell } from '@/components/ui/table';
import { OperationBadge } from '@/components/OperationBadge';
import { CdcMessage, getFullyQualifiedTableName } from '@/types/cdc';

/**
 * CDC Message Row Component
 *
 * Displays a single CDC message in a table row with:
 * - Operation badge (color-coded: INSERT=green, UPDATE=blue, DELETE=red)
 * - Table name (database.schema.table format)
 * - Timestamp (formatted for readability)
 * - Before/After data preview (JSON snippets)
 *
 * Accessibility:
 * - Semantic table structure with proper ARIA roles
 * - Screen reader friendly timestamps
 * - Keyboard navigation support
 * - WCAG 2.1 AA compliant color contrast
 *
 * Props:
 * - message: The CDC message to display
 * - index: Row index for ARIA attributes
 * - onClick: Optional callback when row is clicked (for diff view)
 *
 * Example Usage:
 * ```tsx
 * <CdcMessageRow
 *   message={cdcMessage}
 *   index={0}
 *   onClick={() => handleRowClick(cdcMessage)}
 * />
 * ```
 */

interface CdcMessageRowProps {
  message: CdcMessage;
  index: number;
  onClick?: (message: CdcMessage) => void;
}

/**
 * Format timestamp to 'yyyy-MM-dd HH:mm:ss.SSS' format
 */
function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  const milliseconds = String(date.getMilliseconds()).padStart(3, '0');

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
}

export function CdcMessageRow({ message, index, onClick }: CdcMessageRowProps) {
  const fullyQualifiedTableName = getFullyQualifiedTableName(message.tableInfo);

  // Format timestamp for display
  const formattedTimestamp = formatTimestamp(message.timestamp);

  // Format before/after data preview (first 100 characters)
  const formatDataPreview = (data: Record<string, any> | undefined): string => {
    if (!data) return 'N/A';
    const jsonString = JSON.stringify(data);
    return jsonString.length > 100 ? `${jsonString.substring(0, 100)}...` : jsonString;
  };

  const beforePreview = formatDataPreview(message.beforeData);
  const afterPreview = formatDataPreview(message.afterData);

  // Handle row click
  const handleClick = () => {
    if (onClick) {
      onClick(message);
    }
  };

  // Determine if row is clickable (UPDATE operations can show diff view)
  const isClickable = onClick && message.operation === 'UPDATE';

  return (
    <TableRow
      role="row"
      aria-rowindex={index + 2} // +1 for header, +1 for 1-based indexing
      className={`border-b ${isClickable ? 'cursor-pointer hover:bg-muted/70' : ''}`}
      onClick={handleClick}
      tabIndex={isClickable ? 0 : undefined}
      onKeyDown={(e) => {
        if (isClickable && (e.key === 'Enter' || e.key === ' ')) {
          e.preventDefault();
          handleClick();
        }
      }}
      aria-label={`CDC message: ${message.operation} on ${fullyQualifiedTableName} at ${formattedTimestamp}`}
    >
      {/* Timestamp Cell */}
      <TableCell className="text-xs text-muted-foreground w-40">
        <time dateTime={message.timestamp} aria-label={`Timestamp: ${formattedTimestamp}`}>
          {formattedTimestamp}
        </time>
      </TableCell>

      {/* Operation Badge Cell */}
      <TableCell className="w-32">
        <OperationBadge operation={message.operation} />
      </TableCell>

      {/* Table Name Cell */}
      <TableCell className="font-medium text-sm">
        <span
          aria-label={`Table: ${fullyQualifiedTableName}`}
          className="text-foreground"
        >
          {fullyQualifiedTableName}
        </span>
      </TableCell>

      {/* Before Data Cell */}
      <TableCell className="text-xs text-muted-foreground max-w-xs">
        <div className="truncate" aria-label={`Before data: ${beforePreview}`} title={beforePreview}>
          {beforePreview}
        </div>
      </TableCell>

      {/* After Data Cell */}
      <TableCell className="text-xs text-muted-foreground max-w-xs">
        <div className="truncate" aria-label={`After data: ${afterPreview}`} title={afterPreview}>
          {afterPreview}
        </div>
      </TableCell>

      {/* Transaction ID Cell */}
      <TableCell className="text-xs text-muted-foreground w-24">
        <span aria-label={`Transaction ID: ${message.position.offset.txId || 'N/A'}`}>
          {message.position.offset.txId || 'N/A'}
        </span>
      </TableCell>
    </TableRow>
  );
}
