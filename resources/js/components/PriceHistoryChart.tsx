import { ApexOptions } from 'apexcharts';
import { useEffect, useState } from 'react';
import Chart from 'react-apexcharts';

interface PriceHistoryChartProps {
    gameId: number;
    initialCurrency?: string;
}

export default function PriceHistoryChart({
    gameId,
    initialCurrency = 'USD',
}: PriceHistoryChartProps) {
    const [loading, setLoading] = useState(true);
    const [isBtc, setIsBtc] = useState(false);
    const [currency, setCurrency] = useState(initialCurrency);
    const [chartData, setChartData] = useState<{
        labels: string[];
        datasets: { name: string; data: number[] }[];
    }>({
        labels: [],
        datasets: [],
    });

    useEffect(() => {
        fetchPriceData();
    }, [gameId, isBtc, currency]);

    const fetchPriceData = async () => {
        setLoading(true);
        try {
            const response = await fetch(
                `/api/games/${gameId}/chart?btc=${isBtc}&currency=${currency}`,
            );
            const data = await response.json();
            setChartData(data);
        } catch (error) {
            console.error('Failed to fetch chart data:', error);
        } finally {
            setLoading(false);
        }
    };

    const options: ApexOptions = {
        chart: {
            id: 'price-history',
            toolbar: { show: false },
            zoom: { enabled: false },
            background: 'transparent',
            foreColor: '#94a3b8',
            animations: {
                enabled: true,
                speed: 800,
            },
        },
        theme: {
            mode: 'dark',
        },
        stroke: {
            curve: 'smooth',
            width: 3,
            colors: [isBtc ? '#F7931A' : '#3b82f6'],
        },
        fill: {
            type: 'gradient',
            gradient: {
                shadeIntensity: 1,
                opacityFrom: 0.45,
                opacityTo: 0.05,
                stops: [20, 100],
                colorStops: [
                    {
                        offset: 0,
                        color: isBtc ? '#F7931A' : '#3b82f6',
                        opacity: 0.4,
                    },
                    {
                        offset: 100,
                        color: isBtc ? '#F7931A' : '#3b82f6',
                        opacity: 0,
                    },
                ],
            },
        },
        xaxis: {
            categories: chartData.labels,
            axisBorder: { show: false },
            axisTicks: { show: false },
            labels: {
                show: true,
                rotate: -45,
                style: { fontSize: '10px' },
            },
        },
        yaxis: {
            labels: {
                formatter: (val) => {
                    if (isBtc) return val.toFixed(6) + ' BTC';
                    return new Intl.NumberFormat('en-US', {
                        style: 'currency',
                        currency: currency,
                        maximumFractionDigits: 0,
                    }).format(val);
                },
            },
        },
        grid: {
            borderColor: 'rgba(255, 255, 255, 0.05)',
            strokeDashArray: 4,
        },
        tooltip: {
            theme: 'dark',
            x: { show: true },
            y: {
                formatter: (val) => {
                    if (isBtc) return val.toFixed(8) + ' BTC';
                    return new Intl.NumberFormat('en-US', {
                        style: 'currency',
                        currency: currency,
                    }).format(val);
                },
            },
        },
        markers: {
            size: 4,
            colors: [isBtc ? '#F7931A' : '#3b82f6'],
            strokeColors: '#fff',
            strokeWidth: 2,
            hover: { size: 6 },
        },
    };

    if (loading && chartData.labels.length === 0) {
        return (
            <div className="flex h-64 items-center justify-center rounded-xl bg-gray-900/50">
                <div className="h-8 w-8 animate-spin rounded-full border-2 border-blue-500 border-t-transparent"></div>
            </div>
        );
    }

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <div className="flex rounded-lg border border-white/5 bg-gray-900/80 p-1">
                    <button
                        onClick={() => setIsBtc(false)}
                        className={`rounded-md px-3 py-1 text-xs font-bold transition-all ${!isBtc ? 'bg-blue-600 text-white shadow-lg shadow-blue-900/20' : 'text-gray-400 hover:text-white'}`}
                    >
                        FIAT
                    </button>
                    <button
                        onClick={() => setIsBtc(true)}
                        className={`rounded-md px-3 py-1 text-xs font-bold transition-all ${isBtc ? 'bg-[#F7931A] text-white shadow-lg shadow-orange-900/20' : 'text-gray-400 hover:text-white'}`}
                    >
                        BITCOIN
                    </button>
                </div>

                <div className="text-xs font-medium text-gray-500">
                    {isBtc
                        ? 'All prices rebased to BTC ⚡'
                        : `Price trend in ${currency}`}
                </div>
            </div>

            <div className="h-[300px] w-full">
                <Chart
                    options={options}
                    series={chartData.datasets}
                    type="area"
                    height="100%"
                />
            </div>
        </div>
    );
}
