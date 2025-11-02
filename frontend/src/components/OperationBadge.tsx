'use client';

import { Badge } from '@/components/ui/badge';
import { Operation } from '@/types/cdc';

/**
 * Operation Badge Component
 *
 * Displays a color-coded badge for CDC operation types:
 * - INSERT: Green (hsl(142.1 76.2% 36.3%))
 * - UPDATE: Blue (hsl(221.2 83.2% 53.3%))
 * - DELETE: Red (hsl(0 84.2% 60.2%))
 *
 * Accessibility:
 * - Uses semantic colors with sufficient contrast (WCAG 2.1 AA)
 * - Screen readers announce operation type
 * - Aria-label provides context
 *
 * Props:
 * - operation: The CDC operation type (INSERT/UPDATE/DELETE)
 * - className: Optional additional CSS classes
 *
 * Example Usage:
 * ```tsx
 * <OperationBadge operation={Operation.INSERT} />
 * <OperationBadge operation={Operation.UPDATE} className="ml-2" />
 * ```
 */

interface OperationBadgeProps {
  operation: Operation;
  className?: string;
}

export function OperationBadge({ operation, className }: OperationBadgeProps) {
  // Get color and variant based on operation type
  const getVariant = (op: Operation): 'default' | 'secondary' | 'destructive' | 'outline' => {
    switch (op) {
      case Operation.INSERT:
        return 'default'; // Will be styled green
      case Operation.UPDATE:
        return 'secondary'; // Will be styled blue
      case Operation.DELETE:
        return 'destructive'; // Will be styled red
      default:
        return 'outline';
    }
  };

  // Get custom color class for operation
  const getColorClass = (op: Operation): string => {
    switch (op) {
      case Operation.INSERT:
        return 'bg-green-500 hover:bg-green-600 text-white';
      case Operation.UPDATE:
        return 'bg-blue-500 hover:bg-blue-600 text-white';
      case Operation.DELETE:
        return 'bg-red-500 hover:bg-red-600 text-white';
      default:
        return '';
    }
  };

  const variant = getVariant(operation);
  const colorClass = getColorClass(operation);

  return (
    <Badge
      variant={variant}
      className={`${colorClass} font-semibold ${className || ''}`}
      aria-label={`Operation type: ${operation}`}
    >
      {operation}
    </Badge>
  );
}
