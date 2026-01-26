import { ApexOptions } from 'apexcharts';
import { useMemo } from 'react';
import Chart from 'react-apexcharts';

import type { GameShowPrice } from '@/types';

interface PriceDisparityChartProps {
    prices: GameShowPrice[];
}

const formatBtc = (amount: number, digits = 8) => {
    return `${amount.toFixed(digits)} BTC`;
};

const formatRetailerLabel = (price: GameShowPrice) => {
    const retailer = price.retailer || 'Retailer';
    if (!price.country_code) {
        return retailer;
    }

    return `${retailer} (${price.country_code})`;
};

export default function PriceDisparityChart({
    prices,
}: PriceDisparityChartProps) {
    const filteredPrices = useMemo(() => {
        return prices
            .filter((price) => price.amount > 0 && price.btc_amount !== null)
            .slice()
            .sort((a, b) => (a.btc_amount ?? 0) - (b.btc_amount ?? 0));
    }, [prices]);

    if (filteredPrices.length === 0) {
        return (
            <div className="flex h-64 items-center justify-center rounded-xl bg-gray-900/50">
                <div className="text-sm text-gray-400">
                    No valid prices to compare yet.
                </div>
            </div>
        );
    }

    const minPrice = filteredPrices[0]?.btc_amount ?? 0;
    const maxPrice = filteredPrices[filteredPrices.length - 1]?.btc_amount ?? 0;
    const spread = maxPrice - minPrice;

    const series = [
        {
            name: 'Price',
            data: filteredPrices.map((price) => price.btc_amount ?? 0),
        },
    ];

    const options: ApexOptions = {
        chart: {
            id: 'price-disparity',
            toolbar: {
                show: true,
                tools: {
                    download: true,
                    selection: true,
                    zoom: true,
                    zoomin: true,
                    zoomout: true,
                    pan: true,
                    reset: true,
                },
                autoSelected: 'zoom',
            },
            zoom: {
                enabled: true,
                type: 'x',
                autoScaleYaxis: true,
            },
            background: 'transparent',
            foreColor: '#94a3b8',
            animations: {
                enabled: true,
                speed: 700,
            },
        },
        theme: {
            mode: 'dark',
        },
        plotOptions: {
            bar: {
                columnWidth: '55%',
                borderRadius: 6,
                dataLabels: {
                    position: 'top',
                },
            },
        },
        colors: ['#38bdf8'],
        dataLabels: {
            enabled: true,
            formatter: (value) => formatBtc(Number(value), 4),
            offsetY: -20,
            style: {
                fontSize: '10px',
                colors: ['#94a3b8'],
            },
        },
        xaxis: {
            categories: filteredPrices.map(formatRetailerLabel),
            axisBorder: { show: false },
            axisTicks: { show: false },
            labels: {
                show: true,
                rotate: -35,
                rotateAlways: false,
                hideOverlappingLabels: true,
                style: { fontSize: '10px' },
            },
        },
        yaxis: {
            labels: {
                formatter: (value) => formatBtc(value, 6),
            },
        },
        grid: {
            borderColor: 'rgba(255, 255, 255, 0.05)',
            strokeDashArray: 4,
        },
        tooltip: {
            theme: 'dark',
            custom: ({ series, seriesIndex, dataPointIndex, w }) => {
                const price = filteredPrices[dataPointIndex];
                return `
                    <div class="px-3 py-2 bg-gray-900 border border-gray-700 rounded-lg">
                        <div class="font-semibold text-white mb-1">${formatRetailerLabel(price)}</div>
                        <div class="text-sm text-gray-300">
                            <div>BTC: ${formatBtc(price.btc_amount ?? 0, 8)}</div>
                            <div>Fiat: ${new Intl.NumberFormat(undefined, { style: 'currency', currency: price.currency }).format(price.amount)}</div>
                            ${price.discount_percent > 0 ? `<div class="text-red-400">Discount: -${price.discount_percent}%</div>` : ''}
                        </div>
                    </div>
                `;
            },
        },
        annotations: {
            yaxis: [
                {
                    y: minPrice,
                    borderColor: '#22c55e',
                    label: {
                        text: 'Lowest price',
                        style: {
                            background: '#22c55e',
                            color: '#0f172a',
                        },
                    },
                },
                {
                    y: maxPrice,
                    borderColor: '#ef4444',
                    label: {
                        text: 'Highest price',
                        style: {
                            background: '#ef4444',
                            color: '#fff',
                        },
                    },
                },
            ],
        },
    };

    return (
        <div className="space-y-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="text-xs font-medium text-gray-400">
                    Lowest {formatBtc(minPrice, 8)} • Highest{' '}
                    {formatBtc(maxPrice, 8)} • Spread {formatBtc(spread, 8)}
                </div>
                <div className="text-xs text-gray-500">
                    Rebased to BTC (TradingView → Bybit → Forex fallback)
                </div>
            </div>

            <div className="rounded-lg border border-white/10 bg-gray-900/30 p-2 text-xs text-gray-400">
                💡 Use toolbar controls to zoom, pan, and analyze price
                disparities in detail
            </div>

            <div className="h-[500px] w-full">
                <Chart
                    options={options}
                    series={series}
                    type="bar"
                    height="100%"
                />
            </div>
        </div>
    );
}
