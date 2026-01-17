import { GamePrice } from '@/types';

export default function PriceTable({ prices }: { prices: GamePrice[] }) {
    return (
        <div className="overflow-hidden rounded-lg bg-gray-100 dark:bg-gray-700">
            <table className="w-full text-left">
                <thead className="bg-gray-200 dark:bg-gray-600">
                    <tr>
                        <th className="p-3">Retailer</th>
                        <th className="p-3">Price</th>
                        <th className="p-3">Link</th>
                    </tr>
                </thead>
                <tbody>
                    {prices && prices.length > 0 ? (
                        prices.map((price) => (
                            <tr
                                key={price.id}
                                className="border-t border-gray-200 dark:border-gray-600"
                            >
                                <td className="p-3">{price.retailer}</td>
                                <td className="p-3">
                                    {price.amount_minor > 0
                                        ? `${(price.amount_minor / 100).toFixed(2)} ${price.currency}`
                                        : 'Check Website'}
                                </td>
                                <td className="p-3">
                                    {price.url ? (
                                        <a
                                            href={price.url}
                                            target="_blank"
                                            rel="noreferrer"
                                            className="text-blue-600 hover:underline"
                                        >
                                            Visit
                                        </a>
                                    ) : (
                                        '-'
                                    )}
                                </td>
                            </tr>
                        ))
                    ) : (
                        <tr>
                            <td
                                colSpan={3}
                                className="p-3 text-center text-gray-500"
                            >
                                No prices found.
                            </td>
                        </tr>
                    )}
                </tbody>
            </table>
        </div>
    );
}
